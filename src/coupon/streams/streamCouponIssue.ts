import { Observable } from 'rxjs';
import { Db } from 'mongodb';
import { CouponIssue, UserPrefrences } from '../../generated/coupon_stream';
import { STREAM_TYPE } from 'src/types';
import { DEFAUlT_SETTINGS, Language } from 'src/config/constant';
import { TRACKED_STATUS, NOT_TRACKED_STATUS } from 'src/config/constant';
import { LoggerService } from '@nestjs/common';



export function streamCouponIssues(
  userPrefrences: UserPrefrences,
  db: Db,
  logger: LoggerService
): Observable<CouponIssue> {
  return new Observable((subscriber) => {
    const streamMetrics = {
      startTime: Date.now(),
      initialDocumentsCount: 0,
      changeEventsCount: 0,
      lastEventTime: Date.now(),
      errors: 0,
      statusTransitions: new Map<string, number>()
    };



    const languageCode = userPrefrences?.languageCode || DEFAUlT_SETTINGS.LANGUAGE_CODE;
    const brightness = userPrefrences?.brightness || DEFAUlT_SETTINGS.BRIGHTNESS;

    logger.log('Stream initialization', {
      context: 'streamCouponIssues',
      userPreferences: {
        languageCode,
        brightness,
        originalLanguage: userPrefrences?.languageCode,
        originalBrightness: userPrefrences?.brightness
      }
    });



    (async () => {
      try {
        const fetchStartTime = Date.now();
        const issueCouponDocuments = db.collection('couponIssues').find({ status: { $in: TRACKED_STATUS } });

        for await (const document of issueCouponDocuments) {
          streamMetrics.initialDocumentsCount++;
          logger.log('Initial document emission', {
            context: 'streamCouponIssues',
            documentId: document._id,
            status: document.status,
            businessId: document.businessId,
            documentNumber: streamMetrics.initialDocumentsCount,
            elapsedTime: Date.now() - fetchStartTime
          });
          subscriber.next(mapCouponIssue(document, languageCode, brightness, STREAM_TYPE.BASE));
        }

        await issueCouponDocuments.close();
        logger.log('Initial fetch completed', {
          context: 'streamCouponIssues',
          documentsProcessed: streamMetrics.initialDocumentsCount,
          fetchDuration: Date.now() - fetchStartTime,
          memoryUsage: process.memoryUsage()
        });

        const changeStreamStartTime = Date.now();
        const changeStream = db.collection('couponIssues').watch([], { fullDocument: 'updateLookup' });


        logger.log('Change stream established', {
          context: 'streamCouponIssues',
          setupTime: Date.now() - changeStreamStartTime
        });
        changeStream.on('change', (change: any) => {

          if (!change.fullDocument){
            logger.warn('Change event without full document', {
              context: 'streamCouponIssues',
              operationType: change.operationType,
              documentId: change.documentKey?._id
            });
          return;
          }

          const previousStatus = change?.updateDescription?.removedFields?.includes('status')
          ? null
          : change?.updateDescription?.updatedFields?.status;

        const newStatus = change.fullDocument?.status;

          const transitionKey = `${previousStatus || 'null'}->${newStatus}`;
          streamMetrics.statusTransitions.set(
            transitionKey, 
            (streamMetrics.statusTransitions.get(transitionKey) || 0) + 1
          );
          logger.log('Change event processing', {
            context: 'streamCouponIssues',
            operationType: change.operationType,
            documentId: change.fullDocument._id,
            statusTransition: transitionKey,
            totalChanges: streamMetrics.changeEventsCount,
            timeSinceStart: Date.now() - streamMetrics.startTime
          });

          let streamType: STREAM_TYPE;
          let mappedIssue: CouponIssue | null = null;

         

          const wasTracked = previousStatus && TRACKED_STATUS.includes(previousStatus);
          const isNowTracked = TRACKED_STATUS.includes(newStatus);
          const isNowNotTracked = NOT_TRACKED_STATUS.includes(newStatus);

          switch (change.operationType) {
            case 'update':
              if (wasTracked && isNowTracked) {
                streamType = STREAM_TYPE.UPDATE;
                mappedIssue = mapCouponIssue(change.fullDocument, languageCode, brightness, streamType);
              } else if (wasTracked && isNowNotTracked) {
                streamType = STREAM_TYPE.REMOVED;
                mappedIssue = mapCouponIssue(change.fullDocument, languageCode, brightness, streamType);
              } else if (!wasTracked && isNowTracked) {
                streamType = STREAM_TYPE.INSERT;
                mappedIssue = mapCouponIssue(change.fullDocument, languageCode, brightness, streamType);
              } else if (!wasTracked && isNowNotTracked) {
                streamType = STREAM_TYPE.REMOVED;
                mappedIssue = mapCouponIssue(change.fullDocument, languageCode, brightness, streamType);
              }

              break;
          }

          if (mappedIssue) {
            subscriber.next(mappedIssue);
          }
        });

        changeStream.on('error', (error: any) => {
          streamMetrics.errors++;
          
          logger.error('Change stream error', {
            context: 'streamCouponIssues',
            error: {
              message: error.message,
              stack: error.stack,
              code: error.code
            },
            metrics: {
              totalErrors: streamMetrics.errors,
              uptime: Date.now() - streamMetrics.startTime,
              lastEventAge: Date.now() - streamMetrics.lastEventTime
            }
          });
          console.error('Change stream error:', error);
          subscriber.error(error);
        });

        subscriber.add(() => {
          const streamDuration = Date.now() - streamMetrics.startTime;
          
          logger.log('Stream cleanup', {
            context: 'streamCouponIssues',
            metrics: {
              duration: streamDuration,
              initialDocuments: streamMetrics.initialDocumentsCount,
              changeEvents: streamMetrics.changeEventsCount,
              errors: streamMetrics.errors,
              statusTransitions: Object.fromEntries(streamMetrics.statusTransitions),
              averageChangesPerMinute: (streamMetrics.changeEventsCount / (streamDuration / 1000 / 60)).toFixed(2),
              memoryUsage: process.memoryUsage()
            }
          });
          console.log('Cleaning up change stream');
          changeStream.close();
        });
      } catch (error) {
        streamMetrics.errors++;
        logger.error('Stream operation error', {
          context: 'streamCouponIssues',
          error: {
            message: error.message,
            stack: error.stack,
            code: error.code
          },
          metrics: {
            totalErrors: streamMetrics.errors,
            uptime: Date.now() - streamMetrics.startTime,
            processedDocuments: streamMetrics.initialDocumentsCount + streamMetrics.changeEventsCount
          }
        });
        console.error('Error fetching or streaming data:', error);
        subscriber.error(error);
      }
    })();
  });
}

function mapCouponIssue(doc: any, languageCode: string, brightness: string, streamType: STREAM_TYPE): CouponIssue {
  return {
    id: doc._id ? doc._id.toString() : '',
    drawId: doc.drawId,
    businessContractId: doc.businessContractId,
    deliveryAvailable: doc.deliveryAvailable,
    deliveryContactPhone: doc.deliveryContactPhone,
    title: doc.title?.[languageCode] || doc.title?.[Language.DEFAULT] || 'Unknown',
    image: doc.image?.[brightness]?.[languageCode] || doc.image?.[brightness]?.[Language.DEFAULT] || '',
    descriptionFile: doc.descriptionFile?.[languageCode] || doc.descriptionFile?.[Language.DEFAULT] || '',
    activeAt: doc.activeAt,
    endAt: doc.endAt,
    expireAt: doc.expireAt,
    zoneIds: doc.zoneIds,
    initialAmount: doc.initialAmount,
    currency: doc.currency,
    purchasePriceAmount: doc.purchasePriceAmount,
    discountAmount: doc.discountAmount,
    sellPriceAmount: doc.sellPriceAmount,
    ticketPriceAmount: doc.ticketPriceAmount,
    grandDrawMultiplier: doc.grandDrawMultiplier,
    couponsSource: doc.couponsSource,
    couponsCsvPath: doc.couponsCsvPath,
    additionalCouponsCsvPath: doc.additionalCouponsCsvPath,
    arrangement: doc.arrangement,
    couponsPrefix: doc.couponsPrefix,
    businessId: doc.businessId,
    status: doc.status,
    createdAt: doc.createdAt,
    type: doc.type,
    amountExpired: doc.amountExpired,
    additionalAmount: doc.additionalAmount,
    lastIncrId: doc.lastIncrId,
    nextCodeIncrId: doc.nextCodeIncrId,
    RawPath:doc._rawPath,
    restrictions: doc.restrictions?.[languageCode] || doc.restrictions?.[Language.DEFAULT] || '',
    methodsOfRedemption: doc.methodsOfRedemption,
    amountUsed: doc.amountUsed,
    amountSold: doc.amountSold,
    restrictedBranchIds : doc?.restrictedBranchIds,
    streamType: streamType,
  };
}
