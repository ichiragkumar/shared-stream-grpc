import { Observable } from 'rxjs';
import { Db } from 'mongodb';
import { ActiveBusinessesStreamResponse, UserPrefrences } from '../../generated/coupon_stream';
import { STREAM_TYPE } from 'src/types';
import { DEFAUlT_SETTINGS, NOT_TRACKED_CONTRACT_TYPES, VALID_CONTRACT_TYPES } from 'src/config/constant';
import { LoggerService } from 'src/logger/logger.service';


export function streamActiveBusinessesWithContractTypes(
  userPrefrences: UserPrefrences,
  db: Db,
  logger: LoggerService,
): Observable<ActiveBusinessesStreamResponse> {
  return new Observable(subscriber => {

    const languageCode = userPrefrences?.languageCode || DEFAUlT_SETTINGS.LANGUAGE_CODE;
    const brightness = userPrefrences?.brightness || DEFAUlT_SETTINGS.BRIGHTNESS;


    const requestId = logger.initializeRequest('StreamActiveBusinessesStream', 'Unknown', 'Unknown', 'Unknown', 'Unknown');
    logger.log('Stream initialization', {
      context: 'streamActiveBusinessesWithContractTypes',
      userPreferences: {
        languageCode,
        brightness,
        originalLanguage: userPrefrences?.languageCode,
        originalBrightness: userPrefrences?.brightness
      },
      requestId
    });

    (async () => {
      try {
        const fetchStartTime = Date.now();
        const initialDocuments = await db.collection('businesses').find({ contractTypes: { $in: VALID_CONTRACT_TYPES } }).toArray();
        for (const document of initialDocuments) {
          logger.log('Initial document emission', {
            context: 'streamActiveBusinessesWithContractTypes',
            documentId: document._id,
            businessId: document._id.toString(),
            documentNumber: initialDocuments.indexOf(document) + 1,
            elapsedTime: Date.now() - fetchStartTime,
            requestId
          });
          subscriber.next(mapBusiness(document, languageCode, brightness, STREAM_TYPE.BASE));
        }
        logger.log('Initial fetch completed', {
          context: 'streamActiveBusinessesWithContractTypes',
          requestId,
          documentsProcessed: initialDocuments.length,
          fetchDuration: Date.now() - fetchStartTime
        });



        const changeStream = db.collection('businesses').watch([], { fullDocument: 'updateLookup' });
        logger.log('Change stream established', {
          context: 'streamActiveBusinessesWithContractTypes',
          requestId
        });
        

        changeStream.on('change', async (change: any) => {
          if (!change.fullDocument) {
            logger.warn('Change event missing full document', {
              context: 'streamActiveBusinessesWithContractTypes',
              requestId,
              operationType: change.operationType,
              documentId: change.documentKey?._id?.toString()
            });
            return;
          }

          let streamType: STREAM_TYPE;
          let mappedIssue: ActiveBusinessesStreamResponse | null = null;

          const contractTypes = change.fullDocument?.contractTypes || [];
          const hasTrackedType = contractTypes.some(type => VALID_CONTRACT_TYPES.includes(type));
          const hasOnlyNotTracked = contractTypes.length > 0 && contractTypes.every(type => NOT_TRACKED_CONTRACT_TYPES.includes(type));
          const isNowEmpty = contractTypes.length === 0;

          if (hasTrackedType) {
            streamType = change.operationType === 'insert' ? STREAM_TYPE.INSERT : STREAM_TYPE.UPDATE;
            mappedIssue = mapBusiness(change.fullDocument, languageCode, brightness, streamType);
          } else if (hasOnlyNotTracked || isNowEmpty) {
            streamType = STREAM_TYPE.REMOVED;
            mappedIssue = mapBusiness(change.fullDocument, languageCode, brightness, streamType);
          }

          if (mappedIssue) {
            logger.log('Change event processed', {
              context: 'streamActiveBusinessesWithContractTypes',
              requestId,
              operationType: change.operationType,
              documentId: change.fullDocument._id.toString(),
            });
            subscriber.next(mappedIssue);
          }
        });

        changeStream.on('error', error => {
          logger.error('Change stream error')
          console.error('Change stream error:', error);
          subscriber.error(error);
        });

        subscriber.add(() => {
          logger.log('Stream cleanup', {
            context: 'streamActiveBusinessesWithContractTypes',
            requestId
          });
          console.log('Cleaning up change stream');
          changeStream.close();
        });

      } catch (error) {
        logger.error('Stream operation error', error)
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
