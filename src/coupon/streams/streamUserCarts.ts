import { LoggerService } from '@nestjs/common';
import { Db, ObjectId } from 'mongodb';
import { Observable } from 'rxjs';
import { roundFloat } from 'src/config/constant';
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

    let previousItems: any[] = [];

    (async () => {
      try {
        const fetchStartTime = Date.now();

        console.log(userId);
        const userCartDocument = await db
          .collection('carts')
          .findOne({ userId: new ObjectId(userId) });

        if (userCartDocument && userCartDocument.items?.length) {
          streamMetrics.initialDocumentsCount = userCartDocument.items.length;
          previousItems = userCartDocument.items;

          logger.log('Initial document emission', {
            context: 'streamUserCarts',
            documentId: userCartDocument._id.toString(),
            elapsedTime: Date.now() - fetchStartTime,
          });

          const items: UserCartStreamItem[] = userCartDocument.items.map((item: any) =>
            mapUserCartResponse(item, STREAM_TYPE.BASE)
          );

          subscriber.next({ items, streamType: STREAM_TYPE.BASE });
        } else {
          logger.warn('No user cart found', { context: 'streamUserCarts', userId });
          subscriber.next({ items: [], streamType: STREAM_TYPE.BASE });
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

      const updatedFields = change.updateDescription?.updatedFields || {};
      const currentItems = change.fullDocument.items || [];
      
      // Check if items array has been modified
      if (updatedFields.items !== undefined || 
          Object.keys(updatedFields).some(key => key.startsWith('items.')) ||
          change.updateDescription?.removedFields?.some((field: string) => field.startsWith('items.'))) {

        logger.log('Items array was modified', {
          context: 'streamUserCarts',
          previousLength: previousItems.length,
          currentLength: currentItems.length,
        });

        // CASE 1: Check for deleted items
        const previousItemIds = new Set(previousItems.map(item => item.itemId?.toString()));
        const currentItemIds = new Set(currentItems.map((item: any) => item.itemId?.toString()));
        
        const deletedItemIds = Array.from(previousItemIds).filter(id => !currentItemIds.has(id));
        
        if (deletedItemIds.length > 0) {
          logger.log('Items were deleted', {
            context: 'streamUserCarts',
            deletedItemIds,
          });
          
          // Only emit the deleted items
          const deletedItems = previousItems.filter(item => 
            deletedItemIds.includes(item.itemId?.toString())
          );
          
          subscriber.next({
            items: deletedItems.map(item => mapUserCartResponse(item, STREAM_TYPE.DELETE)),
            streamType: STREAM_TYPE.DELETE,
          });
          
          // Update previous state
          previousItems = currentItems;
          return;
        }
        
        // CASE 2: Check for added items
        const newItems = currentItems.filter((item: any) => 
          !previousItems.some(prevItem => prevItem.itemId?.toString() === item.itemId?.toString())
        );
        
        if (newItems.length > 0) {
          logger.log('New items were added', {
            context: 'streamUserCarts',
            newItemIds: newItems.map((item: any) => item.itemId?.toString()),
          });
          
          // Emit only the new items
          subscriber.next({
            items: newItems.map((item: any) => mapUserCartResponse(item, STREAM_TYPE.INSERT)),
            streamType: STREAM_TYPE.INSERT,
          });
          
          // Update previous state
          previousItems = currentItems;
          return;
        }
        
        // CASE 3: Check for updated items
        let modifiedItems: any[] = [];
        
        if (updatedFields.items !== undefined) {
          // If the entire array was replaced but no items were added or deleted,
          // find which items were modified by comparing with previous state
          modifiedItems = currentItems.filter((item: any) => {
            const prevItem = previousItems.find(p => p.itemId?.toString() === item.itemId?.toString());
            return prevItem && JSON.stringify(item) !== JSON.stringify(prevItem);
          });
        } else {
          // For dot-notation updates (e.g., items.0.amount), extract the modified items
          const modifiedIndices = new Set<number>();
          
          Object.keys(updatedFields)
            .filter(key => key.startsWith('items.'))
            .forEach(key => {
              const parts = key.split('.');
              if (parts.length >= 2) {
                const index = parseInt(parts[1], 10);
                if (!isNaN(index)) {
                  modifiedIndices.add(index);
                }
              }
            });
          
          modifiedItems = Array.from(modifiedIndices)
            .filter(index => index < currentItems.length)
            .map(index => currentItems[index]);
        }
        
        if (modifiedItems.length > 0) {
          logger.log('Items were updated', {
            context: 'streamUserCarts',
            modifiedItemIds: modifiedItems.map((item: any) => item.itemId?.toString()),
          });
          
          // Emit only the modified items
          subscriber.next({
            items: modifiedItems.map((item) => mapUserCartResponse(item, STREAM_TYPE.UPDATE)),
            streamType: STREAM_TYPE.UPDATE,
          });
        }
        
        // Update previous state
        previousItems = currentItems;
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
  const taxAmount = item.taxAmount !== undefined ? roundFloat(item.taxAmount) : undefined;
  const feePrice = item.feePrice !== undefined ? roundFloat(item.feePrice) : undefined;
  console.log('Rounded feePrice:', feePrice);
  console.log('Rounded taxAmount:', taxAmount);

  return {
    itemId: item.itemId?.toString() ?? null,
    amount: item.amount ?? 0,
    purchasePrice: item.purchasePrice ?? 0,
    currency: item.currency ?? null,
    feePrice: item.feePrice !== undefined ? roundFloat(item.feePrice) : undefined,
    taxAmount: item.taxAmount !== undefined ? roundFloat(item.taxAmount) : undefined,
    streamType,
  };
};