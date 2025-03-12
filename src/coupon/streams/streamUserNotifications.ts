import { Observable } from 'rxjs';
import { Db, ObjectId } from 'mongodb';
import { LoggerService } from '@nestjs/common';
import { STREAM_TYPE } from 'src/types';
import { UserIdOptional, UserNotificationStreamResponse } from 'src/generated/coupon_stream';

const DEFAULT_USER_ID = new ObjectId('000000000000000000000000');

export function streamUserNotifications(
    db: Db,
    data: UserIdOptional,
    logger: LoggerService
): Observable<UserNotificationStreamResponse> {
    return new Observable<UserNotificationStreamResponse>(subscriber => {
        let { userId } = data;

        const streamMetrics = {
            startTime: Date.now(),
            initialDocumentsCount: 0,
            changeEventsCount: 0,
            errors: 0,
        };

        logger.log('Stream initialization', {
            context: 'streamUserNotifications',
            userId,
        });

        (async () => {
            try {
                const fetchStartTime = Date.now();
                const PREVIOUS_DATE = new Date(Date.now() - 24 * 60 * 60 * 1000);


                const queryForAll = {
                    userId: DEFAULT_USER_ID,
                    createdAt: { $gte: PREVIOUS_DATE }
                };


                const queryForUser = {
                    userId: new ObjectId(userId),
                    isRead: false,
                    createdAt: { $gte: PREVIOUS_DATE }
                };


                const notifications = await db
                    .collection('notifications')
                    .find({ $or: [queryForUser, queryForAll] })
                    .sort({ createdAt: -1 })
                    .limit(50)
                    .toArray();


                if (notifications.length === 0) {
                    logger.warn('No user notification found', {
                        context: 'streamUserNotifications',
                        userId,
                    });

                    subscriber.next(
                        {
                            id: '',
                            isRead: false,
                            createdAt: '',
                            title: '',
                            body: '',
                            topic: '',
                            screen: '',
                            userId: '',
                            streamType: STREAM_TYPE.BASE,
                        }
                    );
                    subscriber.complete();
                    return;
                }

                streamMetrics.initialDocumentsCount = notifications.length;
                logger.log('Initial documents emission', {
                    context: 'streamUserNotifications',
                    count: notifications.length,
                    elapsedTime: Date.now() - fetchStartTime,
                });


                for (const notificationDocument of notifications) {
                    subscriber.next(mapUserNotificationResponse(notificationDocument, STREAM_TYPE.BASE));
                }


                const changeStream = db.collection('notifications').watch(
                    [
                        {
                            $match: {
                                $or: [
                                    { 'fullDocument.userId': new ObjectId(userId) },
                                    { 'fullDocument.userId': DEFAULT_USER_ID }
                                ],
                                operationType: { $in: ['insert', 'update'] },
                                'fullDocument.createdAt': { $gte: PREVIOUS_DATE } 
                            },
                        }
                    ],
                    { fullDocument: 'updateLookup' }
                );



                logger.log('Change stream established', {
                    context: 'streamUserNotifications',
                    userId,
                });


                changeStream.on('change', (change: any) => {                
                    switch (change.operationType) {
                        case 'insert':
                        case 'update':
                            if (!change.fullDocument) {
                                logger.warn('Change event without full document', {
                                    context: 'streamUserNotifications',
                                    operationType: change.operationType,
                                    documentId: change.documentKey?._id,
                                });
                                return;
                            }
                
                            streamMetrics.changeEventsCount++;
                
                            const streamType = change.operationType === 'insert' ? STREAM_TYPE.INSERT : STREAM_TYPE.UPDATE;
                            const userNotification = change.fullDocument;
                
                            logger.log('Change event processing', {
                                context: 'streamUserNotifications',
                                operationType: change.operationType,
                                documentId: userNotification._id,
                                totalChanges: streamMetrics.changeEventsCount,
                                timeSinceStart: Date.now() - streamMetrics.startTime,
                            });
                
                            subscriber.next(mapUserNotificationResponse(userNotification, streamType));
                            break;
                
                        case 'delete':
            
                        default:
                            logger.warn('Unhandled operation type', {
                                context: 'streamUserNotifications',
                                operationType: change.operationType,
                            });
                            break;
                    }
                });


                changeStream.on('error', (error: any) => {
                    streamMetrics.errors++;
                    logger.error('Change stream error', {
                        context: 'streamUserNotifications',
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

                    subscriber.error(new Error('An error occurred while streaming user notifications.'));
                });


                subscriber.add(() => {
                    logger.log('Stream cleanup', {
                        context: 'streamUserNotifications',
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
            } catch (error) {
                streamMetrics.errors++;
                logger.error('Stream operation error', {
                    context: 'streamUserNotifications',
                    error: {
                        message: error.message,
                        stack: error.stack,
                        code: error.code,
                    },
                    metrics: {
                        totalErrors: streamMetrics.errors,
                        uptime: Date.now() - streamMetrics.startTime,
                        processedDocuments:
                            streamMetrics.initialDocumentsCount + streamMetrics.changeEventsCount,
                    },
                });

                subscriber.error(error);
            }
        })();
    });
}


function mapUserNotificationResponse(
    document: any,
    streamType: number
): UserNotificationStreamResponse {
    return {
        id: document._id?.toString() || '',
        isRead: document.isRead ?? false,
        createdAt: document.createdAt?.$date || document.createdAt || '',
        title: document.content?.title?.en || '',
        body: document.content?.body?.en || '',
        topic: document.topic || '',
        screen: document.screen || '',
        userId: document.userId?.toString() || '',
        streamType,
    };
}


