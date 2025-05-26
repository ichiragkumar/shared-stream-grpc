import { Observable } from 'rxjs';
import { Db } from 'mongodb';
import { CouponIssueWithBusiness, UserPrefrences } from 'src/generated/coupon_stream';
import { safeParseDate, STREAM_TYPE } from 'src/types';
import { DEFAULT_COUPON_ISSUE_WITH_BUSINESS, DEFAUlT_SETTINGS, BUSINESS_VALID_STATUS, BUSINESS_NOT_TRACKED_STATUS } from 'src/config/constant';
import { LoggerService } from '@nestjs/common';





export function streamActiveCouponIssuesWithBusiness(db: Db, userPrefrences: UserPrefrences, logger: LoggerService): Observable<CouponIssueWithBusiness> {
  return new Observable(subscriber => {
    const languageCode = userPrefrences?.languageCode || DEFAUlT_SETTINGS.LANGUAGE_CODE;
    const brightness = userPrefrences?.brightness || DEFAUlT_SETTINGS.BRIGHTNESS;



   
    const streamMetrics = {
      startTime: Date.now(),
      initialDocumentsCount: 0,
      changeEventsCount: 0,
      errors: 0
    };

    logger.log('Stream initialized', {
      context: 'streamActiveCouponIssuesWithBusiness',
      languageCode,
      brightness
    });


    const pipeline = [
      { $match: { status: { $in: BUSINESS_VALID_STATUS } } },
      {
        $lookup: {
          from: 'businesses',
          localField: 'businessId',
          foreignField: '_id',
          as: 'business'
        }
      },
      { $unwind: { path: '$business', preserveNullAndEmptyArrays: true } }
    ];

    

    (async () => {
      try {
        const fetchStartTime = Date.now();

        const initialResults = await db.collection('couponIssues').aggregate(pipeline).toArray();
        for (const document of initialResults) {
          subscriber.next(mapToCouponIssue(document, languageCode, brightness, STREAM_TYPE.BASE));
        }
        

        logger.log('Initial fetch completed', {
          context: 'streamActiveCouponIssuesWithBusiness',
          documentsProcessed: streamMetrics.initialDocumentsCount,
          fetchDuration: Date.now() - fetchStartTime
        });

        const changeStreamStartTime = Date.now();
        const changeStream = db.collection('couponIssues').watch([], { fullDocument: 'updateLookup' });
        logger.log('Change stream established', {
          context: 'streamActiveCouponIssuesWithBusiness',
          setupTime: Date.now() - changeStreamStartTime
        });
        changeStream.on('change', async (change: any) => {
          if (!change.fullDocument){
            logger.warn('Change event without full document', {
              context: 'streamActiveCouponIssuesWithBusiness',
              operationType: change.operationType,
              documentId: change.documentKey?._id
            });
          return;
          }


          try {
            let streamType: STREAM_TYPE;
            let mappedIssue: CouponIssueWithBusiness | null = null;

            const previousStatus = change?.updateDescription?.updatedFields?.status ?? change?.fullDocumentBeforeChange?.status;
            const newStatus = change.fullDocument?.status;
            if(BUSINESS_NOT_TRACKED_STATUS.includes(newStatus)){
              streamType = STREAM_TYPE.REMOVED;
              mappedIssue = mapToCouponIssue(change.fullDocument, languageCode, brightness, streamType);
            }
            const wasTracked = previousStatus && BUSINESS_VALID_STATUS.includes(previousStatus);
            const isNowTracked = BUSINESS_VALID_STATUS.includes(newStatus);


            if (wasTracked && isNowTracked) {
              streamType = STREAM_TYPE.UPDATE;
              mappedIssue = mapToCouponIssue(change.fullDocument, languageCode, brightness, streamType);
            } else if (!wasTracked && isNowTracked) {
              streamType = STREAM_TYPE.INSERT;
              mappedIssue = mapToCouponIssue(change.fullDocument, languageCode, brightness, streamType);
            }

            if (mappedIssue) {
              streamMetrics.changeEventsCount++;
              logger.log('Change event processed', {
                context: 'streamMoreCouponRequestsService',
                documentId: change.fullDocument._id,
                operationType: change.operationType,
                totalChanges: streamMetrics.changeEventsCount
              });
              subscriber.next(mappedIssue);
            }
          } catch (error) {
            streamMetrics.errors++;
            logger.error('Error processing change stream event', {
              context: 'streamActiveCouponIssuesWithBusiness',
              error: {
                message: error.message,
                stack: error.stack
              },
              metrics: { totalErrors: streamMetrics.errors }
            });
            console.error('Error processing change stream event:', error);
            subscriber.error(error);
          }
        });

        changeStream.on('error', (error) => {
          streamMetrics.errors++;
          logger.error('Change stream error', {
            context: 'streamActiveCouponIssuesWithBusiness',
            error: {
              message: error.message,
              stack: error.stack
            },
            metrics: { totalErrors: streamMetrics.errors }
          });

          console.error('Change stream error:', error);
          subscriber.error(error);
        });

        subscriber.add(() => {
          logger.log('Stream cleanup', {
            context: 'streamActiveCouponIssuesWithBusiness',
            metrics: {
              duration: Date.now() - streamMetrics.startTime,
              initialDocuments: streamMetrics.initialDocumentsCount,
              changeEvents: streamMetrics.changeEventsCount,
              errors: streamMetrics.errors
            }
          });
          console.log('Cleaning up change stream');
          changeStream.close();
        });
      } catch (error) {

        streamMetrics.errors++;
        logger.error('Stream initialization error', {
          context: 'streamActiveCouponIssuesWithBusiness',
          error: {
            message: error.message,
            stack: error.stack
          },
          metrics: { totalErrors: streamMetrics.errors }
        });
        console.error('Stream initialization error:', error);
        subscriber.error(error);
      }
    })();
  });
}


const mapToCouponIssue = (
  document: any, 
  lang: string, 
  bright: string, 
  streamType: STREAM_TYPE
): CouponIssueWithBusiness => {
  return {
    couponIssueId: document._id?.toString() || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.couponIssueId,
    businessId: document.businessId?.toString() || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.businessId,
    couponName: document.title?.[lang] || document.title?.en || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.couponName,
    businessName: document.business?.title?.[lang] || document.business?.title?.en || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.businessName,
    status: document.status || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.status,
    logo: document.business?.logo?.[bright]?.[lang] || 
          document.business?.logo?.[bright]?.en || 
          document.business?.logo?.dark?.[lang] || 
          document.business?.logo?.dark?.en || 
          DEFAULT_COUPON_ISSUE_WITH_BUSINESS.logo,
    categories: Array.isArray(document.business?.categories) ? document.business.categories : [],
    endsAt: document.endAt ? safeParseDate(document.endAt) : DEFAULT_COUPON_ISSUE_WITH_BUSINESS.endsAt as any,
    amountLeft: typeof document.initialAmount === 'number' ? Math.max(0, document.initialAmount - (document.amountSold || 0)) : DEFAULT_COUPON_ISSUE_WITH_BUSINESS.amountLeft,
    type: document.type || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.type,
    priceAmount: Number(document.priceAmount) || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.priceAmount,
    currency: document.currency || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.currency,
    drawId: document.drawId || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.drawId,
    sellPriceAmount: Number(document.sellPriceAmount) || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.sellPriceAmount,
    restrictedBranchIds: Array.isArray(document.zoneIds) ? document.zoneIds : DEFAULT_COUPON_ISSUE_WITH_BUSINESS.restrictedBranchIds,
    drawNumbers: Array.isArray(document.drawNumbers) ? document.drawNumbers : DEFAULT_COUPON_ISSUE_WITH_BUSINESS.drawNumbers,
    descriptionFile: document.descriptionFile?.[lang] || document.descriptionFile?.en || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.descriptionFile,
    purchasePriceAmount: Number(document.purchasePriceAmount) || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.purchasePriceAmount,
    arrangement: Number(document.arrangement) || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.arrangement,
    streamType: streamType || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.streamType
  };
};