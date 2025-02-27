import { Observable } from 'rxjs';
import { Db, ObjectId } from 'mongodb';
import { Balance, WalletBalanceResponse, User } from 'src/generated/coupon_stream';
import { STREAM_TYPE } from 'src/types';
import { LoggerService } from '@nestjs/common';


export function streamWalletBalance(db: Db, data: User,logger:LoggerService): Observable<WalletBalanceResponse> {
  return new Observable<WalletBalanceResponse>(subscriber => {


    const { userId } = data;
    const streamMetrics = {
      startTime: Date.now(),
      initialDocumentsCount: 0,
      changeEventsCount: 0,
      errors: 0,
    };

    logger.log('Stream initialization', {
      context: 'streamWalletBalance',
      userId,
    });


    (async () => {
      try {
        const fetchStartTime = Date.now();
            const userWalletDocument = await db.collection('wallets').findOne({
              userId: new ObjectId(userId)
            });

        if (!userWalletDocument) {
          logger.warn('No wallet found for user', {
            context: 'streamWalletBalance',
            userId,
          });

          subscriber.next({
            availableBalances: { USD: 0, EGP: 0 },
            blockedBalances: { USD: 0, EGP: 0 },
            streamType: STREAM_TYPE.BASE,
          });
        } else {


          const availableBalances: Balance = userWalletDocument.availableBalances || { USD: 0, EGP: 0 };
          const blockedBalances: Balance = userWalletDocument.blockedBalances || { USD: 0, EGP: 0 };

          streamMetrics.initialDocumentsCount++;

          logger.log('Initial wallet balance emission', {
            context: 'streamWalletBalance',
            userId,
            availableBalances,
            blockedBalances,
            elapsedTime: Date.now() - fetchStartTime,
          });


          subscriber.next({
            availableBalances,
            blockedBalances,
            streamType: STREAM_TYPE.BASE,
          });
        }

        const changeStream = db.collection('wallets').watch(
          [{ $match: { 'fullDocument.userId': new ObjectId(userId) } }],
          { fullDocument: 'updateLookup' }
        );

        logger.log('Change stream established', {
          context: 'streamWalletBalance',
          userId,
        });

        changeStream.on('change', (change: any) => {
          if (!change.fullDocument) {
            logger.warn('Change event without full document', {
              context: 'streamWalletBalance',
              operationType: change.operationType,
              documentId: change.documentKey?._id,
            });
            return;
          }

          streamMetrics.changeEventsCount++;

          let streamType: STREAM_TYPE = STREAM_TYPE.BASE;
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

          const availableBalances = change.fullDocument.availableBalances || { USD: 0, EGP: 0 };
          const blockedBalances = change.fullDocument.blockedBalances || { USD: 0, EGP: 0 };


          logger.log('Change event processing', {
            context: 'streamWalletBalance',
            operationType: change.operationType,
            documentId: change.fullDocument._id,
            totalChanges: streamMetrics.changeEventsCount,
            timeSinceStart: Date.now() - streamMetrics.startTime,
          });
          subscriber.next({ availableBalances, blockedBalances, streamType: streamType });
        });

        changeStream.on('error', (error: any) => {
          streamMetrics.errors++;
          logger.error('Change stream error', {
            context: 'streamWalletBalance',
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
          subscriber.error(new Error('An error occurred while streaming wallet balance updates.'));
        });

        subscriber.add(() => {
          logger.log('Stream cleanup', {
            context: 'streamWalletBalance',
            metrics: {
              duration: Date.now() - streamMetrics.startTime,
              initialDocuments: streamMetrics.initialDocumentsCount,
              changeEvents: streamMetrics.changeEventsCount,
              errors: streamMetrics.errors,
              memoryUsage: process.memoryUsage(),
            },
          });

          console.log('Cleaning up wallet balance change stream');
          changeStream.close();
        });
      } catch (error) {
        streamMetrics.errors++;
        logger.error('Stream operation error', {
          context: 'streamWalletBalance',
          error: {
            message: error.message,
            stack: error.stack,
            code: error.code,
          },
          metrics: {
            totalErrors: streamMetrics.errors,
            uptime: Date.now() - streamMetrics.startTime,
            processedDocuments: streamMetrics.initialDocumentsCount + streamMetrics.changeEventsCount,
          },
        });
        console.error('Error streaming wallet balance:', error);
        subscriber.error(error);
      }
    })();
  });
}
