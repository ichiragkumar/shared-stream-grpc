import { Observable } from 'rxjs';
import { Db, ObjectId } from 'mongodb';
import { LoggerService } from '@nestjs/common';
import { STREAM_TYPE } from 'src/types';
import { UserIdOptional, UserNotificationStreamResponse } from 'src/generated/coupon_stream';

const PREVIOUS_DATE = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);

export function streamUserNotifications(
    db: Db,
    data: UserIdOptional,
    logger: LoggerService
): Observable<UserNotificationStreamResponse> {
    return new Observable<UserNotificationStreamResponse>(subscriber => {
        let { userId } = data;
        userId = userId || '-';


        console.log("i am here ")
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

                const query: any = userId === '-'
                    ? { createdAt: { $gte: PREVIOUS_DATE } }
                    : {
                          $or: [
                              { userId: new ObjectId(userId), isRead: false },
                              { createdAt: { $gte: PREVIOUS_DATE } },
                          ],
                      };

                const notifications = await db
                    .collection('notifications')
                    .find(query)
                    .sort({ createdAt: -1 })
                    .limit(50)
                    .toArray();

                console.log("all docuemnt ids are", notifications.map(x => x._id))

                if (notifications.length === 0) {
                    logger.warn('No user notification found for user', {
                        context: 'streamUserNotifications',
                        userId,
                    });

                    subscriber.next({
                        id: '',
                        isRead: false,
                        createdAt: '',
                        title: '',
                        body: '',
                        image: '',
                        topic: '',
                        screen: '',
                        userId: '',
                        streamType: STREAM_TYPE.BASE,
                    });

                    subscriber.complete();
                    return;
                }

                streamMetrics.initialDocumentsCount = notifications.length;
                logger.log('Initial documents emission', {
                    context: 'streamUserNotifications',
                    count: notifications.length,
                    elapsedTime: Date.now() - fetchStartTime,
                });

                for (const doc of notifications) {
                    console.log(doc._id)
                    subscriber.next(mapUserNotificationResponse(doc, STREAM_TYPE.BASE));
                }


                const changeStream = db.collection('notifications').watch(
                    [
                        {
                            $match: {
                                $or: [
                                    { 'fullDocument.userId': new ObjectId(userId) },
                                    { 'fullDocument.createdAt': { $gte: PREVIOUS_DATE } },
                                ],
                            },
                        },
                        {
                            $project: {
                                _id: 1,
                                isRead: 1,
                                createdAt: 1,
                                'content.title': 1,
                                'content.body': 1,
                                topic: 1,
                                screen: 1,
                                userId: 1,
                            },
                        },
                    ],
                    { fullDocument: 'updateLookup' }
                );

                logger.log('Change stream established', {
                    context: 'streamUserNotifications',
                    userId,
                });

                changeStream.on('change', (change: any) => {
                    if (!change.fullDocument) {
                        logger.warn('Change event without full document', {
                            context: 'streamUserNotifications',
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

                    const userNotification = change.fullDocument;

                    logger.log('Change event processing', {
                        context: 'streamUserNotifications',
                        operationType: change.operationType,
                        documentId: userNotification._id,
                        totalChanges: streamMetrics.changeEventsCount,
                        timeSinceStart: Date.now() - streamMetrics.startTime,
                    });
                    subscriber.next(mapUserNotificationResponse(userNotification, streamType));
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

                // ✅ Cleanup on completion
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


function mapUserNotificationResponse(document: any, streamType: number): UserNotificationStreamResponse {
    return {
        id: document._id?.toString() || '',
        isRead: document.isRead ?? false,
        createdAt: document.createdAt?.$date || document.createdAt || '',
        title: document.content?.title?.en || '',
        body: document.content?.body?.en || '',
        image: '',
        topic: document.topic || '',
        screen: document.screen || '',
        userId: document.userId?.toString() || '',
        streamType,
    };
}
