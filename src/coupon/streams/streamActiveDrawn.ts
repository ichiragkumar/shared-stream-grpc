import { Observable } from 'rxjs';
import { Db } from 'mongodb';
import {
  ActiveDrawnResponse,
  UserPrefrences,
} from 'src/generated/coupon_stream';
import {
  ACTIVE_DRAWN_STATUS,
  DEFAUlT_SETTINGS,
  DRAW_NOT_TRACKED_STATUS,
} from 'src/config/constant';
import { LoggerService } from '@nestjs/common';
import { STREAM_TYPE } from 'src/types';

export function streamActiveDrawn(
  db: Db,
  userPrefrences: UserPrefrences,
  logger: LoggerService,
): Observable<ActiveDrawnResponse> {
  return new Observable((subscriber) => {
    const languageCode =
      userPrefrences?.languageCode || DEFAUlT_SETTINGS.LANGUAGE_CODE;
    const brightness =
      userPrefrences?.brightness || DEFAUlT_SETTINGS.BRIGHTNESS;

    const streamMetrics = {
      startTime: Date.now(),
      initialDocumentsCount: 0,
      changeEventsCount: 0,
      removeEventsCount: 0,
      errors: 0,
    };

    logger.log('Stream Initialized', {
      context: 'streamActiveDrawn',
      userPreferences: userPrefrences,
    });

    (async () => {
      try {
        const fetchStartTime = Date.now();

        const initialDocuments = db
          .collection('draws')
          .find({ status: { $in: ACTIVE_DRAWN_STATUS } });
        for await (const document of initialDocuments) {
          subscriber.next(
            mapActiveDrawn(document, languageCode, brightness, 0),
          );
        }

        logger.log('Initial fetch completed', {
          context: 'streamActiveDrawn',
          documentsProcessed: streamMetrics.initialDocumentsCount,
          fetchDuration: Date.now() - fetchStartTime,
          memoryUsage: process.memoryUsage(),
        });
      } catch (error) {
        streamMetrics.errors++;
        logger.error('Error fetching initial documents', {
          context: 'streamActiveDrawn',
          error: {
            message: error.message,
            stack: error.stack,
          },
          metrics: {
            totalErrors: streamMetrics.errors,
          },
        });

        console.error('Error fetching initial documents:', error);
        subscriber.error(error);
      }
    })();

    const changeStream = db
      .collection('draws')
      .watch([], { fullDocument: 'updateLookup' });

    logger.log('Change stream established', {
      context: 'streamActiveDrawn',
    });

    changeStream.on('change', (change: any) => {
      if (!change.fullDocument) {
        logger.warn('Change event without full document', {
          context: 'streamActiveDrawn',
          operationType: change.operationType,
          documentId: change.documentKey?._id,
        });

        return;
      }

      const currentStatus = change.fullDocument.status;
      if (DRAW_NOT_TRACKED_STATUS.includes(currentStatus)) {
        logger.log('Removing coupon from stream due to status change', {
          status: currentStatus,
          couponId: change.fullDocument.id,
        });

        subscriber.next(
          mapActiveDrawn(
            change.fullDocument,
            languageCode,
            brightness,
            STREAM_TYPE.DELETE,
          ),
        );
        return;
      }

      streamMetrics.changeEventsCount++;
      logger.log('Change event processing', {
        context: 'streamActiveDrawn',
        operationType: change.operationType,
        documentId: change.fullDocument._id,
        totalChanges: streamMetrics.changeEventsCount,
        timeSinceStart: Date.now() - streamMetrics.startTime,
      });

      subscriber.next(
        mapActiveDrawn(change.fullDocument, languageCode, brightness, 1),
      );
    });

    changeStream.on('error', (error) => {
      streamMetrics.errors++;
      logger.error('Change stream error', {
        context: 'streamActiveDrawn',
        error: {
          message: error.message,
          stack: error.stack,
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
        context: 'streamActiveDrawn',
        metrics: {
          duration: Date.now() - streamMetrics.startTime,
          initialDocuments: streamMetrics.initialDocumentsCount,
          changeEvents: streamMetrics.changeEventsCount,
          removeEvents: streamMetrics.removeEventsCount,
          errors: streamMetrics.errors,
          memoryUsage: process.memoryUsage(),
        },
      });
      console.log('Cleaning up change stream');
      changeStream.close();
    });
  });
}

function mapActiveDrawn(
  doc: any,
  languageCode: string,
  brightness: string,
  streamType: number,
): ActiveDrawnResponse {
  return {
    id: doc._id ?? null,
    contractId: doc.contractId ?? null,
    businessId: doc.businessId ?? null,
    type: doc.type || '',
    subtype: doc.subtype || '',
    currency: doc.currency || '',
    title: doc.title?.[languageCode] || 'Unknown Title',
    openAt: doc.openAt?.$date || doc.openAt || null,
    predrawStartAt: doc.predrawStartAt?.$date || doc.predrawStartAt || null,
    drawStartAt: doc.drawStartAt?.$date || doc.drawStartAt || null,
    contestsStartAt: doc.contestsStartAt?.$date || doc.contestsStartAt || null,
    descriptionFile: doc.descriptionFile?.[languageCode] || 'No Description',
    logo: doc.logo?.[brightness]?.[languageCode] || '',
    amountOfNumbersByParticipant: doc.amountOfNumbersByParticipant ?? 0,
    grandDrawFreeTicketSpendingsAmount:
      doc.grandDrawFreeTicketSpendingsAmount ?? null,
    drawNumbersCount: doc.drawNumbersCount ?? 0,
    participantsCount: doc.participantsCount ?? 0,
    amountOfChosenNumbers: doc.amountOfChosenNumbers ?? 0,
    totalPrizesValue: doc.totalPrizesValue ?? 0,
    totalPrizesAmount: doc.totalPrizesAmount ?? 0,
    createdAt: doc.createdAt?.$date || doc.createdAt || null,
    status: doc.status || '',
    specialEvent: doc.specialEvent
      ? {
          cardColor: doc.specialEvent.cardColor,
          title: doc.specialEvent.title?.[languageCode],
          shortDescription: doc.specialEvent.shortDescription?.[languageCode],
        }
      : undefined,
    streamType: streamType,
  };
}
