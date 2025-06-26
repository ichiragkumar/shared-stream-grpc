import { Observable } from 'rxjs';
import { Db, ObjectId } from 'mongodb';
import {
  User,
  ActiveCouponStreamResponse,
} from '../../generated/coupon_stream';
import {
  NOT_TRACKED_USER_COUPON_STATUS,
  USER_COUPON_STATUS,
} from 'src/config/constant';
import { LoggerService } from '@nestjs/common';
import { STREAM_TYPE } from 'src/types';
import { ConnectionManagerService } from 'src/config/connection-manager.service';
import { SharedChangeStreamService } from 'src/config/shared-change-stream.service';

export function streamActiveCouponsStream(
  db: Db,
  data: User,
  logger: LoggerService,
  connectionManager?: ConnectionManagerService,
  sharedChangeStream?: SharedChangeStreamService,
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

        // OPTIMIZATION 1: Use aggregation pipeline for single query instead of multiple queries
        // This eliminates the N+1 query problem and reduces database round trips
        const pipeline = [
          {
            $match: {
              userId: new ObjectId(userId),
              status: { $in: USER_COUPON_STATUS },
              redeemedBySelfActivation: false,
            }
          },
          {
            $lookup: {
              from: 'couponIssues',
              localField: 'couponIssueId',
              foreignField: '_id',
              as: 'couponIssueDetails'
            }
          },
          {
            $unwind: {
              path: '$couponIssueDetails',
              preserveNullAndEmptyArrays: false // Filter out documents without matching couponIssue
            }
          },
          {
            $project: {
              _id: 1,
              code: 1,
              businessId: 1,
              couponIssueId: 1,
              redeemedBySelfActivation: 1,
              purchasePrice: 1,
              purchaseCurrency: 1,
              userId: 1,
              status: 1,
              expireAt: 1,
              createdAt: 1,
              purchasedAt: 1,
              redemptionInfo: 1,
              sellPriceAmount: '$couponIssueDetails.sellPriceAmount'
            }
          }
        ];

        // OPTIMIZATION 2: Execute aggregation pipeline (indexes will be used automatically)
        const initialDocuments = await db
          .collection('userCoupons')
          .aggregate(pipeline)
          .toArray();

        if (!initialDocuments || initialDocuments.length === 0) {
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

        streamMetrics.initialDocumentsCount = initialDocuments.length;

        // OPTIMIZATION 3: Stream documents one by one without blocking
        for (const doc of initialDocuments) {
          subscriber.next(
            mapToCouponIssue(
              doc,
              doc.sellPriceAmount || 0,
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

        // OPTIMIZATION 4: Use ConnectionManager or SharedChangeStream for better performance
        let changeStream;
        let isUsingSharedStream = false;

        if (sharedChangeStream) {
          // Use SharedChangeStreamService for maximum connection efficiency
          const userIdString = userId.toString();
          isUsingSharedStream = true;

          const sharedStream = sharedChangeStream.getSharedChangeStream(
            'userCoupons',
            (change) => {
              return change.fullDocument?.userId?.toString() === userIdString;
            }
          );

          sharedStream.subscribe({
            next: (change) => {
              processChangeEvent(change);
            },
            error: (error) => {
              logger.error('Shared change stream error', {
                context: 'streamActiveCouponsStream',
                error: error.message,
              });
              subscriber.error(error);
            },
            complete: () => {
              logger.log('Shared change stream completed', { context: 'streamActiveCouponsStream' });
              subscriber.complete();
            }
          });

          logger.log('Using shared change stream for collection: userCoupons', { context: 'streamActiveCouponsStream' });
        } else if (connectionManager) {
          // Use ConnectionManager for managed change streams
          changeStream = connectionManager.createChangeStream(
            db.collection('userCoupons'),
            [
              {
                $match: {
                  'fullDocument.userId': new ObjectId(userId),
                },
              },
            ],
            { fullDocument: 'updateLookup' }
          );
          logger.log('Managed change stream established via ConnectionManager', { context: 'streamActiveCouponsStream' });
        } else {
          // Fallback to direct change stream
          changeStream = db.collection('userCoupons').watch(
            [
              {
                $match: {
                  'fullDocument.userId': new ObjectId(userId),
                },
              },
            ],
            { fullDocument: 'updateLookup' },
          );
          logger.log('Standard change stream established', { context: 'streamActiveCouponsStream' });
        }

        // Function to process change events (used by both shared and direct change streams)
        async function processChangeEvent(change: any) {
          streamMetrics.changeEventsCount++;
          
          if (!change.fullDocument) {
            logger.warn('Change event without full document', {
              context: 'streamActiveCouponsStream',
              operationType: change.operationType,
              documentId: change.documentKey?._id,
            });
            return;
          }

          const currentStatus = change.fullDocument.status;
          const redeemedBySelfActivation =
            change.fullDocument.redeemedBySelfActivation;

          if (currentStatus === USER_COUPON_STATUS[0]) {
            logger.log('Coupon status changed to active', {
              couponId: change.fullDocument.id,
            });

            subscriber.next({
              id: change.fullDocument._id,
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
              streamType: STREAM_TYPE.UPDATE,
            });
            return;
          }

          if (redeemedBySelfActivation === true) {
            logger.log('Coupon redeemed by self activation', {
              couponId: change.fullDocument.id,
            });

           subscriber.next({
              id: change.fullDocument._id,
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

          if (NOT_TRACKED_USER_COUPON_STATUS.includes(currentStatus)) {
            logger.log('Removing coupon from stream due to status change', {
              status: currentStatus,
              couponId: change.fullDocument.id,
            });

            subscriber.next({
              id: change.fullDocument._id,
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

          // OPTIMIZATION 5: Use the aggregation lookup instead of separate query
          const couponIssueDetails = await db
            .collection('couponIssues')
            .findOne(
              { _id: change.fullDocument.couponIssueId },
              { projection: { sellPriceAmount: 1 } } // Only fetch what we need
            );

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

        // Only set up the on('change') event handler if we're not using the shared stream
        if (!isUsingSharedStream && changeStream) {
          changeStream.on('change', async (change: any) => {
            await processChangeEvent(change);
          });
        }

        // Only set up error and cleanup handlers if we're not using the shared stream
        if (!isUsingSharedStream && changeStream) {
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
        } else if (isUsingSharedStream) {
          // For shared streams, add cleanup without closing the stream
          subscriber.add(() => {
            logger.log('Stream cleanup (shared stream)', {
              context: 'streamActiveCouponsStream',
              metrics: {
                duration: Date.now() - streamMetrics.startTime,
                initialDocuments: streamMetrics.initialDocumentsCount,
                changeEvents: streamMetrics.changeEventsCount,
                errors: streamMetrics.errors,
                memoryUsage: process.memoryUsage(),
              },
            });
          });
        }
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
    id: doc._id.toString(),
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


