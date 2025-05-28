import { Observable } from 'rxjs';
import { Db, ObjectId } from 'mongodb';
import {
  User,
  ActiveCouponStreamResponse,
} from '../../generated/coupon_stream';
import { NOT_TRACKED_USER_COUPON_STATUS, USER_COUPON_STATUS } from 'src/config/constant';
import { LoggerService } from '@nestjs/common';
import { STREAM_TYPE } from 'src/types';

export function streamActiveCouponsStream(
  db: Db,
  data: User,
  logger: LoggerService,
): Observable<ActiveCouponStreamResponse> {
  return new Observable((subscriber) => {
    const { userId } = data;

    const streamMetrics = {
      startTime: Date.now(),
      initialDocumentsCount: 0,
      changeEventsCount: 0,
      errors: 0,
    };

    logger.log('Stream Initialized', {
      context: 'streamActiveCouponsStream',
      userId,
    });

    (async () => {
      try {
        const fetchStartTime = Date.now();

        const userCouponDocument = {
          userId: new ObjectId(userId),
          status: { $in: USER_COUPON_STATUS },
          redeemedBySelfActivation: false,
        };

        const initialDocuments = await db
          .collection('userCoupons')
          .find(userCouponDocument)
          .toArray();

        if (!initialDocuments) {
          logger.warn('No matching coupons for user', {
            context: 'streamActiveCouponsStream',
            userId,
          });
          subscriber.next({
            id: 'User ID Does not Exist',
            status: 'No matching coupons',
            redemptionInfo: {
              redeemedByBusinessManagerId: '',
              methodOfRedemption: '',
            },
            code: '',
            businessId: '',
            couponIssueId: '',
            redeemedBySelfActivation: false,
            purchasePrice: 0,
            purchaseCurrency: '',
            userId,
            expireAt: '',
            createdAt: '',
            purchasedAt: '',
            sellPriceAmount: 0,
            streamType: STREAM_TYPE.BASE,
          });
          return;
        }

        const couponIssueIds = initialDocuments
          .map((userCouponEachDocument) => userCouponEachDocument.couponIssueId)
          .filter((couponIssueId) => couponIssueId);

        const couponIssues = await db
          .collection('couponIssues')
          .find({ _id: { $in: couponIssueIds } })
          .toArray();

        const couponIssueMap = new Map(
          couponIssues.map((issue) => [issue._id.toString(), issue]),
        );

        for await (const doc of initialDocuments) {
          const couponIssueDocument = couponIssueMap.get(
            doc.couponIssueId.toString(),
          );

          if (!couponIssueDocument) {
            logger.warn('Coupon issue document not found', {
              context: 'streamActiveCouponsStream',
              couponIssueId: doc.couponIssueId,
            });
            return;
          }
          subscriber.next(
            mapToCouponIssue(
              doc,
              couponIssueDocument.sellPriceAmount || 0,
              STREAM_TYPE.BASE,
            ),
          );
        }
        logger.log('Initial fetch completed', {
          context: 'streamActiveCouponsStream',
          documentsProcessed: streamMetrics.initialDocumentsCount,
          fetchDuration: Date.now() - fetchStartTime,
          memoryUsage: process.memoryUsage(),
        });

        const changeStream = db.collection('userCoupons').watch(
          [
            {
              $match: {
                'fullDocument.userId': new ObjectId(userId),
                'fullDocument.redeemedBySelfActivation': false,
              },
            },
          ],
          { fullDocument: 'updateLookup' },
        );
        logger.log('Change stream established', {
          context: 'streamActiveCouponsStream',
          userId,
        });

        changeStream.on('change', async (change: any) => {
          if (!change.fullDocument) {
            logger.warn('Change event without full document', {
              context: 'streamActiveCouponsStream',
              operationType: change.operationType,
              documentId: change.documentKey?._id,
            });
            return;
          }

          if (change.fullDocument) {
            const currentStatus = change.fullDocument.status;

            if (NOT_TRACKED_USER_COUPON_STATUS.includes(currentStatus)) {
              logger.log('Removing coupon from stream due to status change', {
                status: currentStatus,
                couponId: change.fullDocument.id,
              });

              subscriber.next({
                id: change.fullDocument.id,
                redemptionInfo: change.fullDocument.redemptionInfo || null,
                code: change.fullDocument.code,
                businessId: change.fullDocument.businessId,
                couponIssueId: change.fullDocument.couponIssueId,
                redeemedBySelfActivation: change.fullDocument.redeemedBySelfActivation,
                purchasePrice: change.fullDocument.purchasePrice,
                purchaseCurrency: change.fullDocument.purchaseCurrency,
                userId: change.fullDocument.userId,
                status: change.fullDocument.status,
                expireAt: change.fullDocument.expireAt,
                createdAt: change.fullDocument.createdAt,
                purchasedAt: change.fullDocument.purchasedAt,
                sellPriceAmount: change.fullDocument.sellPriceAmount,
                streamType: STREAM_TYPE.DELETE,
              });

              return;
            }

            const couponIssueDetails = await db
              .collection('couponIssues')
              .findOne({
                _id: change.fullDocument.couponIssueId,
              });

            if (!couponIssueDetails) {
              logger.warn('Coupon issue details not found', {
                context: 'streamActiveCouponsStream',
                couponIssueId: change.fullDocument.couponIssueId,
              });
              return;
            }

            logger.log('Change event processing', {
              context: 'streamActiveCouponsStream',
              operationType: change.operationType,
              documentId: change.fullDocument.id,
              totalChanges: streamMetrics.changeEventsCount,
              timeSinceStart: Date.now() - streamMetrics.startTime,
            });

            subscriber.next(
              mapToCouponIssue(
                change.fullDocument,
                couponIssueDetails.sellPriceAmount || 0,
                STREAM_TYPE.BASE,
              ),
            );
          }
        });

        changeStream.on('error', (error: any) => {
          streamMetrics.errors++;
          logger.error('Change stream error', {
            context: 'streamActiveCouponsStream',
            error: {
              message: error.message,
              stack: error.stack,
              code: error.code,
            },
            metrics: {
              totalErrors: streamMetrics.errors,
              uptime: Date.now() - streamMetrics.startTime,
            },
          });

          console.error('Change stream error:', error);
          subscriber.error(error);
        });

        subscriber.add(() => {
          logger.log('Stream cleanup', {
            context: 'streamActiveCouponsStream',
            metrics: {
              duration: Date.now() - streamMetrics.startTime,
              initialDocuments: streamMetrics.initialDocumentsCount,
              changeEvents: streamMetrics.changeEventsCount,
              errors: streamMetrics.errors,
              memoryUsage: process.memoryUsage(),
            },
          });
          changeStream.close();
        });
      } catch (error) {
        streamMetrics.errors++;
        logger.error('Stream operation error', {
          context: 'streamActiveCouponsStream',
          error: {
            message: error.message,
            stack: error.stack,
            code: error.code,
          },
          metrics: {
            totalErrors: streamMetrics.errors,
            uptime: Date.now() - streamMetrics.startTime,
            processedDocuments:
              streamMetrics.initialDocumentsCount +
              streamMetrics.changeEventsCount,
          },
        });
        console.error('Error in streaming:', error);
        subscriber.error(error);
      }
    })();
  });
}

function mapToCouponIssue(
  doc: any,
  sellPriceAmount: number,
  streamType: number,
): ActiveCouponStreamResponse {
  return {
    id: doc._id?.toString(),
    redemptionInfo: doc.redemptionInfo || null,
    code: doc.code,
    businessId: doc.businessId,
    couponIssueId: doc.couponIssueId,
    redeemedBySelfActivation: doc.redeemedBySelfActivation,
    purchasePrice: doc.purchasePrice,
    purchaseCurrency: doc.purchaseCurrency,
    userId: doc.userId,
    status: doc.status,
    expireAt: doc.expireAt,
    createdAt: doc.createdAt,
    purchasedAt: doc.purchasedAt,
    sellPriceAmount: parseFloat(sellPriceAmount.toFixed(2)),
    streamType: streamType,
  };
}

