import { Observable } from 'rxjs';
import { Db, ObjectId } from 'mongodb';
import { LoggerService } from '@nestjs/common';
import { TicketStreamResponse, User } from 'src/generated/coupon_stream';

export function streamUserTickets(db: Db, data: User, logger: LoggerService): Observable<TicketStreamResponse> {
  return new Observable(subscriber => {
    const { userId } = data;

    const streamMetrics = {
      startTime: Date.now(),
      initialDocumentsCount: 0,
      changeEventsCount: 0,
      errors: 0,
    };

    logger.log('Stream Initialized', {
      context: 'streamUserTickets',
      userId,
    });

    (async () => {
      try {
        const fetchStartTime = Date.now();


        const ticketDocuments = db.collection('tickets')
          .find({ userId:new ObjectId(userId) });

        let hasTickets = false;
        for await (const document of ticketDocuments) {
          hasTickets = true;
          subscriber.next(mapTicketResponse(document,0));
        }

        if (!hasTickets) {
          logger.warn('No tickets found for user', {
            context: 'streamUserTickets',
            userId,
          });


          subscriber.next({
            id: 'No Tickets Found',
            userId,
            drawId: '',
            drawType: '',
            isDrawClosed: false,
            drawNumbers: [],
            createdAt: '',
            status: '',
            winningDrawNumber: '',
            streamType: 0,
          });
        }

        logger.log('Initial fetch completed', {
          context: 'streamUserTickets',
          documentsProcessed: streamMetrics.initialDocumentsCount,
          fetchDuration: Date.now() - fetchStartTime,
          memoryUsage: process.memoryUsage(),
        });
      } catch (error) {
        streamMetrics.errors++;
        logger.error('Error fetching initial documents', {
          context: 'streamUserTickets',
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


    const changeStream = db.collection('tickets').watch(
      [{ $match: { 'fullDocument.userId': new ObjectId(userId) } }],
      { fullDocument: 'updateLookup' }
    );

    logger.log('Change stream established', {
      context: 'streamUserTickets',
      userId,
    });

    changeStream.on('change', (change: any) => {
      if (!change.fullDocument) {
        logger.warn('Change event without full document', {
          context: 'streamUserTickets',
          operationType: change.operationType,
          documentId: change.documentKey?._id,
        });

        return;
      }

      streamMetrics.changeEventsCount++;
      logger.log('Change event processing', {
        context: 'streamUserTickets',
        operationType: change.operationType,
        documentId: change.fullDocument._id,
        totalChanges: streamMetrics.changeEventsCount,
        timeSinceStart: Date.now() - streamMetrics.startTime,
      });

      subscriber.next(mapTicketResponse(change.fullDocument,1));
    });

    changeStream.on('error', error => {
      streamMetrics.errors++;
      logger.error('Change stream error', {
        context: 'streamUserTickets',
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
        context: 'streamUserTickets',
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
}
function mapTicketResponse(document: any, streamType: number): TicketStreamResponse {
  return {
    id: document._id?.toString(),
    userId: document.userId.toString(),
    drawId: document.drawId.toString(),
    drawType: document.drawType,
    isDrawClosed: document.isDrawClosed ?? false,
    drawNumbers: Array.isArray(document.drawNumbers)
    ? document.drawNumbers.map(num => String(num))
    : [], 
    createdAt: document.createdAt?.$date || document.createdAt,
    status: document.status,
    winningDrawNumber: document.winningDrawNumber,
    streamType: streamType,
  };
}
