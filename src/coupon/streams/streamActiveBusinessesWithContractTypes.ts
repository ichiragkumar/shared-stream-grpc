import { Observable } from 'rxjs';
import { Db } from 'mongodb';
import { ActiveBusinessesStreamResponse, LanguageFilter } from '../../generated/coupon_stream';
import { STREAM_TYPE } from 'src/types';


let streamType: STREAM_TYPE;  
export function streamActiveBusinessesWithContractTypes(
  languageFilter: LanguageFilter,
  db: Db
): Observable<ActiveBusinessesStreamResponse> {
  return new Observable(subscriber => {
    const languageCode = languageFilter?.languageCode || 'en';


    db.collection('businesses')
      .find({ contractTypes: { $in: ['vendor', 'advertiser', 'sponsor', 'specialIssue', 'business', 'voucher'] } })
      .forEach(doc => {
        const response = mapBusiness(doc, languageCode, STREAM_TYPE.BASE);
        subscriber.next(response);
      })
      .then(() => {
        const changeStream = db.collection('businesses').watch(
          [
            {
              $match: {
                'fullDocument.contractTypes': { $in: ['vendor', 'advertiser', 'sponsor', 'specialIssue', 'business', 'voucher'] },
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

          const response = mapBusiness(change.fullDocument, languageCode, streamType);
          subscriber.next(response);
        });

        changeStream.on('error', error => {
          console.error('Change stream error:', error);
          subscriber.error(error);
        });

        return () => {
          console.log('Cleaning up change stream');
          changeStream.close();
        };
      })
      .catch(error => {
        console.error('Error fetching initial data:', error);
        subscriber.error(error);
      });
  });
}

function mapBusiness(doc: any, languageCode: string, streamType: STREAM_TYPE): ActiveBusinessesStreamResponse {
  return {
    id: doc._id.toString(),
    title: doc.title?.[languageCode] || doc.title?.['en'] || 'Unknown Title',
    description: doc.description?.[languageCode] || doc.description?.['en'] || 'No Description',
    image: doc.logo?.light?.[languageCode] || doc.logo?.light?.['en'] || null,
    categories: doc.categories || [],
    businessId: doc._id.toString(),
    contractType: doc.contractTypes.join(', '),
    streamType: streamType,
  };
}
