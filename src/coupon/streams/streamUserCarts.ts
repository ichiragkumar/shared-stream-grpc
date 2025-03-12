import { LoggerService } from '@nestjs/common';
import { Db, ObjectId } from 'mongodb';
import { Observable } from 'rxjs';
import { UserCartStreamItem, UserCartStreamResponse } from 'src/generated/coupon_stream';
import { STREAM_TYPE } from 'src/types';

export const streamUserCarts = (
  db: Db,
  user: { userId: string },
  logger: LoggerService,
): Observable<UserCartStreamResponse> => {
  return new Observable((subscriber) => {
    const { userId } = user;

    const streamMetrics = {
      startTime: Date.now(),
      initialDocumentsCount: 0,
      changeEventsCount: 0,
      errors: 0,
    };

    logger.log('Stream initialization', {
      context: 'streamUserCarts',
      userId,
    });

    (async () => {
      try {
        const fetchStartTime = Date.now();

        console.log(userId)
        const userCartDocument = await db
          .collection('carts')
          .findOne({ userId: new ObjectId(userId) });

        if (userCartDocument && userCartDocument.items?.length) {
          streamMetrics.initialDocumentsCount = userCartDocument.items.length;

          logger.log('Initial document emission', {
            context: 'streamUserCarts',
            documentId: userCartDocument._id.toString(),
            elapsedTime: Date.now() - fetchStartTime,
          });

          const items: UserCartStreamItem[] = userCartDocument.items.map((item: any) =>
            mapUserCartResponse(item, STREAM_TYPE.BASE)
          );

          console.log('Initial items:', items);
          subscriber.next({ items, streamType: STREAM_TYPE.BASE });
        } else {
          logger.warn('No user cart found', { context: 'streamUserCarts', userId });
          subscriber.next({ items: [], streamType: STREAM_TYPE.BASE });
          subscriber.complete();
        }
      } catch (error) {
        streamMetrics.errors++;
        logger.error('Error fetching initial document', {
          context: 'streamUserCarts',
          error: {
            message: error.message,
            stack: error.stack,
          },
          metrics: {
            totalErrors: streamMetrics.errors,
          },
        });

        subscriber.error(error);
      }
    })();


    const changeStream = db.collection('carts').watch(
      [
        {
          $match: {
            'fullDocument.userId': new ObjectId(userId),
          },
        },
      ],
      { fullDocument: 'updateLookup' },
    );

    logger.log('Change stream established', { context: 'streamUserCarts' });

    changeStream.on('change', (change: any) => {
      logger.log('Change event received', {
        operationType: change.operationType,
        fullDocument: change.fullDocument,
        updatedFields: change.updateDescription?.updatedFields,
        removedFields: change.updateDescription?.removedFields,
      });

      streamMetrics.changeEventsCount++;

      let streamType: number;
      switch (change.operationType) {
        case 'insert':
          streamType = STREAM_TYPE.INSERT;
          break;
        case 'update':
          streamType = STREAM_TYPE.UPDATE;
          break;
        case 'delete':
          subscriber.next({
            items: [],
            streamType: STREAM_TYPE.DELETE,
          });
          return;
        default:
          logger.warn('Unhandled operation type', {
            context: 'streamUserCarts',
            operationType: change.operationType,
          });
          return;
      }


      const modifiedItems = Object.keys(change.updateDescription?.updatedFields || {})
        .filter((key) => key.startsWith('items.'))
        .map((key) => {
          const index = parseInt(key.split('.')[1], 10);
          return change.fullDocument.items[index];
        });


      const deletedItems = (change.updateDescription?.removedFields || [])
        .filter((key:any) => key.startsWith('items.'))
        .map((key:any) => {
          const index = parseInt(key.split('.')[1], 10);
          const deletedItem = change.fullDocument.items[index];
          return deletedItem
            ? {
                itemId: deletedItem.itemId?.toString() || '',
                amount: 0,
                purchasePrice: 0,
                currency: '',
                feePrice: undefined,
                taxAmount: undefined,
                streamType: STREAM_TYPE.DELETE,
              }
            : null;
        })
        .filter((item) => item !== null) as UserCartStreamItem[];


      const itemsToEmit: UserCartStreamItem[] = [
        ...modifiedItems.map((item) => mapUserCartResponse(item, streamType)),
        ...deletedItems,
      ];

      if (itemsToEmit.length) {
        subscriber.next({
          items: itemsToEmit,
          streamType,
        });
      }
    });


    changeStream.on('error', (error) => {
      streamMetrics.errors++;
      logger.error('Change stream error', {
        context: 'streamUserCarts',
        error: {
          message: error.message,
          stack: error.stack,
        },
        metrics: {
          totalErrors: streamMetrics.errors,
          uptime: Date.now() - streamMetrics.startTime,
        },
      });

      subscriber.error(error);
    });


    subscriber.add(() => {
      logger.log('Stream cleanup', {
        context: 'streamUserCarts',
        metrics: {
          duration: Date.now() - streamMetrics.startTime,
          initialDocuments: streamMetrics.initialDocumentsCount,
          changeEvents: streamMetrics.changeEventsCount,
          errors: streamMetrics.errors,
          memoryUsage: process.memoryUsage(),
        },
      });

      console.log('Cleaning up change stream');
      changeStream.close();
    });
  });
};

const mapUserCartResponse = (
  item: any,
  streamType: number,
): UserCartStreamItem => {
  return {
    itemId: item.itemId?.toString() || '',
    amount: item.amount || 0,
    purchasePrice: item.purchasePrice || 0,
    currency: item.currency || '',
    feePrice: item.feePrice || undefined,
    taxAmount: item.taxAmount || undefined,
    streamType,
  };
};
