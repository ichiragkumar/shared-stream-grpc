import { Observable } from 'rxjs';
import { Db } from 'mongodb';
import { MoreCouponRequest, User } from '../../generated/coupon_stream';
import { LoggerService } from '@nestjs/common';

export function streamMoreCouponRequestsService(
  db: Db,
  data: User,
  logger: LoggerService
): Observable<MoreCouponRequest> {
  return new Observable<MoreCouponRequest>((subscriber) => {
    const { userId } = data;
    const streamMetrics = {
      startTime: Date.now(),
      initialDocumentsCount: 0,
      changeEventsCount: 0,
      errors: 0
    };

    logger.log('Stream initialization', {
      context: 'streamMoreCouponRequestsService',
      userId
    });

    (async () => {
      try {
        const fetchStartTime = Date.now();
        let userCouponDocument = false;
        const userCouponDocuments = db.collection('moreCouponsRequests').find({ userId });
        for await (const document of userCouponDocuments) {
          userCouponDocument = true;
          streamMetrics.initialDocumentsCount++;

          logger.log('Initial document emission', {
            context: 'streamMoreCouponRequestsService',
            documentId: document._id,
            elapsedTime: Date.now() - fetchStartTime
          });


          subscriber.next({
            id: document._id.toString(),
            userId: document.userId,
            couponIssueId: document.couponIssueId,
            createdAt: new Date(document.createdAt).getTime(),
            parentId: document.parentId || '',
          });
        }

        await userCouponDocuments.close();
        logger.log('Initial fetch completed', {
          context: 'streamMoreCouponRequestsService',
          documentsProcessed: streamMetrics.initialDocumentsCount,
          fetchDuration: Date.now() - fetchStartTime
        });



        if (!userCouponDocument) {
          logger.warn('No initial documents found', { context: 'streamMoreCouponRequestsService', userId });

          subscriber.next({
            id: '',
            userId,
            couponIssueId: '',
            createdAt: 0,
            parentId: '',
          });
          subscriber.complete();
          return;
        }

        const changeStreamStartTime = Date.now();
        const changeStream = db.collection('moreCouponsRequests').watch(
          [
            {
              $match: {
                'fullDocument.userId': userId,
              },
            },
          ],
          { fullDocument: 'updateLookup' }
        );

        logger.log('Change stream established', {
          context: 'streamMoreCouponRequestsService',
          setupTime: Date.now() - changeStreamStartTime
        });


        changeStream.on('change', (change: any) => {
          if (!change.fullDocument){
            logger.warn('Change event without full document', {
              context: 'streamMoreCouponRequestsService',
              operationType: change.operationType,
              documentId: change.documentKey?._id
            });
          return;
          }

          streamMetrics.changeEventsCount++;
          const { _id, userId, couponIssueId, createdAt, parentId } = change.fullDocument;

          const moreCouponRequest: MoreCouponRequest = {
            id: _id.toString(),
            userId,
            couponIssueId,
            createdAt: new Date(createdAt).getTime(),
            parentId: parentId || '',
          };


          logger.log('Change event processed', {
            context: 'streamMoreCouponRequestsService',
            documentId: _id,
            totalChanges: streamMetrics.changeEventsCount
          });

          subscriber.next(moreCouponRequest);
        });

        changeStream.on('error', (err: any) => {
          streamMetrics.errors++;
          logger.error('Change stream error', {
            context: 'streamMoreCouponRequestsService',
            error: {
              message: err.message,
              stack: err.stack
            },
            metrics: {
              totalErrors: streamMetrics.errors
            }
          });

          console.error('Change stream error:', err);
          subscriber.error(new Error('An error occurred while streaming changes.'));
        });


        subscriber.add(() => {
          logger.log('Stream cleanup', {
            context: 'streamMoreCouponRequestsService',
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

        logger.error('Stream operation error', {
          context: 'streamMoreCouponRequestsService',
          error: {
            message: error.message,
            stack: error.stack
          },
          metrics: {
            totalErrors: streamMetrics.errors,
            uptime: Date.now() - streamMetrics.startTime
          }
        });
        console.error('Error in streaming:', error);
        subscriber.error(error);
      }
    })();
  });
}
