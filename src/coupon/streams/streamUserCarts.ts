
// import { LoggerService } from '@nestjs/common';
// import { Db, ObjectId } from 'mongodb';
// import { Observable } from 'rxjs';
// import { User, UserCartStreamResponse } from 'src/generated/coupon_stream';
// import { STREAM_TYPE } from 'src/types';

// export const streamUserCarts = (
//   db: Db,
//   user: User,
//   logger: LoggerService,
// ): Observable<UserCartStreamResponse[]> => {
//   return new Observable((subscriber) => {
//     const { userId } = user;

//     const streamMetrics = {
//       startTime: Date.now(),
//       initialDocumentsCount: 0,
//       changeEventsCount: 0,
//       errors: 0,
//     };

//     logger.log('Stream initialization', {
//       context: 'streamUserCarts',
//       userId,
//     });

//     (async () => {
//       try {
//         const fetchStartTime = Date.now();

//         const userCartDocument = await db
//           .collection('carts')
//           .findOne({ userId: new ObjectId(userId) });

//         if (userCartDocument && userCartDocument.items?.length) {
//           streamMetrics.initialDocumentsCount += userCartDocument.items.length;

//           logger.log('Initial document emission', {
//             context: 'streamUserCarts',
//             documentId: userCartDocument._id,
//             elapsedTime: Date.now() - fetchStartTime,
//           });

//           // ✅ Emit all items as a single array
//           subscriber.next(
//             userCartDocument.items.map((item: any) =>
//               mapUserCartResponse(item, STREAM_TYPE.BASE),
//             ),
//           );
//         } else {
//           logger.warn('No user cart found', {
//             context: 'streamUserCarts',
//             userId,
//           });

//           // ✅ Emit an empty array when no cart is found
//           subscriber.next([]);
//         }

//         logger.log('Initial fetch completed', {
//           context: 'streamUserCarts',
//           documentsProcessed: streamMetrics.initialDocumentsCount,
//           fetchDuration: Date.now() - fetchStartTime,
//           memoryUsage: process.memoryUsage(),
//         });
//       } catch (error) {
//         streamMetrics.errors++;
//         logger.error('Error fetching initial document', {
//           context: 'streamUserCarts',
//           error: {
//             message: error.message,
//             stack: error.stack,
//           },
//           metrics: {
//             totalErrors: streamMetrics.errors,
//           },
//         });

//         subscriber.error(error);
//       }
//     })();

//     const changeStream = db.collection('carts').watch(
//       [
//         {
//           $match: {
//             'fullDocument.userId': new ObjectId(userId),
//           },
//         },
//       ],
//       { fullDocument: 'updateLookup' },
//     );

//     logger.log('Change stream established', {
//       context: 'streamUserCarts',
//     });

//     changeStream.on('change', (change: any) => {
//       if (!change.fullDocument || !change.fullDocument.items?.length) {
//         logger.warn('Change event without full document', {
//           context: 'streamUserCarts',
//           operationType: change.operationType,
//           documentId: change.documentKey?._id,
//         });
//         return;
//       }

//       streamMetrics.changeEventsCount++;

//       let streamType: STREAM_TYPE;
//       switch (change.operationType) {
//         case 'insert':
//           streamType = STREAM_TYPE.INSERT;
//           break;
//         case 'update':
//           streamType = STREAM_TYPE.UPDATE;
//           break;
//         case 'delete':
//           streamType = STREAM_TYPE.DELETE;
//           break;
//         default:
//           return;
//       }

//       logger.log('Change event processing', {
//         context: 'streamUserCarts',
//         operationType: change.operationType,
//         documentId: change.fullDocument._id,
//         totalChanges: streamMetrics.changeEventsCount,
//         timeSinceStart: Date.now() - streamMetrics.startTime,
//       });

//       // ✅ Emit all items as a single array on change
//       subscriber.next(
//         change.fullDocument.items.map((item: any) =>
//           mapUserCartResponse(item, streamType),
//         ),
//       );
//     });

//     changeStream.on('error', (error) => {
//       streamMetrics.errors++;
//       logger.error('Change stream error', {
//         context: 'streamUserCarts',
//         error: {
//           message: error.message,
//           stack: error.stack,
//         },
//         metrics: {
//           totalErrors: streamMetrics.errors,
//           uptime: Date.now() - streamMetrics.startTime,
//         },
//       });

//       subscriber.error(error);
//     });

//     subscriber.add(() => {
//       logger.log('Stream cleanup', {
//         context: 'streamUserCarts',
//         metrics: {
//           duration: Date.now() - streamMetrics.startTime,
//           initialDocuments: streamMetrics.initialDocumentsCount,
//           changeEvents: streamMetrics.changeEventsCount,
//           errors: streamMetrics.errors,
//           memoryUsage: process.memoryUsage(),
//         },
//       });

//       console.log('Cleaning up change stream');
//       changeStream.close();
//     });
//   });
// };


// const mapUserCartResponse = (
//   item: any,
//   streamType: STREAM_TYPE,
// ): UserCartStreamResponse => {
//   return {
//     itemId: item.itemId?.toString() || '',
//     amount: item.amount || 0,
//     purchasePrice: item.purchasePrice || 0,
//     currency: item.currency || '',
//     feePrice: item.feePrice || 0,
//     taxAmount: item.taxAmount || 0,
//     streamType,
//   };
// };
