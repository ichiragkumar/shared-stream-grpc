import { Observable } from 'rxjs';
import { Db } from 'mongodb';
import { ActiveBusinessesStreamResponse, UserPrefrences } from '../../generated/coupon_stream';
import { STREAM_TYPE } from 'src/types';
import { DEFAUlT_SETTINGS, validContractTypes } from 'src/config/constant';

export function streamActiveBusinessesWithContractTypes(
  languageFilter: UserPrefrences,
  db: Db
): Observable<ActiveBusinessesStreamResponse> {
  return new Observable(subscriber => {
    const languageCode = languageFilter?.languageCode || DEFAUlT_SETTINGS.LANGUAGE_CODE;
    const brightness = languageFilter?.brightness || DEFAUlT_SETTINGS.BRIGHTNESS;

    (async () => {
      try {
        const initialDocuments = await db.collection('businesses').find({ contractTypes: { $in: validContractTypes } }).toArray();
        for (const doc of initialDocuments) {
          subscriber.next(mapBusiness(doc, languageCode, brightness, STREAM_TYPE.BASE));
        }

        const changeStream = db.collection('businesses').watch(
          [
            {
              $match: {
                'fullDocument.contractTypes': { $in: validContractTypes },
              },
            },
          ],
          { fullDocument: 'updateLookup' }
        );

        changeStream.on('change', async (change: any) => {
          if (!change.fullDocument) return;

          let streamType: STREAM_TYPE;
          switch (change.operationType) {
            case 'insert':
              streamType = STREAM_TYPE.INSERT;
              break;
            case 'update':
              streamType = STREAM_TYPE.UPDATE;
              break;
            default:
              return;
          }

          subscriber.next(mapBusiness(change.fullDocument, languageCode, brightness, streamType));
        });

        changeStream.on('error', error => {
          console.error('Change stream error:', error);
          subscriber.error(error);
        });

        subscriber.add(() => {
          console.log('Cleaning up change stream');
          changeStream.close();
        });

      } catch (error) {
        console.error('Error fetching initial data:', error);
        subscriber.error(error);
      }
    })();
  });
}

function mapBusiness(doc: any, languageCode: string, brightness: string, streamType: STREAM_TYPE): ActiveBusinessesStreamResponse {
  return {
    id: doc._id.toString(),
    title: doc.title?.[languageCode] || doc.title?.en || 'Unknown Title',
    description: doc.description?.[languageCode] || doc.description?.en || 'No Description',
    image: doc.images || '',
    categories: doc.categories || [],
    businessId: doc._id.toString(),
    contractType: Array.isArray(doc.contractTypes) ? doc.contractTypes.join(', ') : '',
    logo: doc.logo?.[brightness]?.[languageCode] || doc.logo?.[brightness]?.['en'] || doc.logo?.[brightness]?.['Unknown Logo'] || doc.logo?.light?.[languageCode] || doc.logo?.light?.['en'] || doc.logo?.light?.['Unknown Logo'] || doc.logo?.dark?.[languageCode] || doc.logo?.dark?.['en'] || doc.logo?.dark?.['Unknown Logo'] || '',
    createdAt: doc.createdAt,
    sponsorshipType: doc.sponsorshipType || '',
    suspended: doc.suspended,
    streamType: streamType,
  };
}
