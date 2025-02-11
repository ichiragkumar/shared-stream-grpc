import { Observable } from 'rxjs';
import { Db } from 'mongodb';
import { ActiveDrawnResponse, UserPrefrences } from 'src/generated/coupon_stream';
import { ACTIVE_DRAWN_STATUS, DEFAUlT_SETTINGS } from 'src/config/constant';
import { LoggerService } from '@nestjs/common';

export function streamActiveDrawn(db: Db, userPrefrences: UserPrefrences, logger: LoggerService): Observable<ActiveDrawnResponse> {
  return new Observable(subscriber => {
    const languageCode = userPrefrences?.languageCode || DEFAUlT_SETTINGS.LANGUAGE_CODE;
    const brightness = userPrefrences?.brightness || DEFAUlT_SETTINGS.BRIGHTNESS;

    const streamMetrics = {
      startTime: Date.now(),
      initialDocumentsCount: 0,
      changeEventsCount: 0,
      removeEventsCount: 0,
      errors: 0,
    };

    logger.log('Stream initialization', {
      context: 'streamActiveDrawn',
      userPreferences: userPrefrences,
    });

    (async () => {
      try {
        const fetchStartTime = Date.now();

        const initialDocuments = db.collection('draws').find({ status: { $in: ACTIVE_DRAWN_STATUS } });
        for await (const document of initialDocuments) {
          streamMetrics.initialDocumentsCount++;
          logger.log('Initial document emission', {
            context: 'streamActiveDrawn',
            documentId: document._id,
            businessId: document.businessId,
            contractId: document.contractId,
            elapsedTime: Date.now() - fetchStartTime,
          });
          subscriber.next(mapActiveDrawn(document, languageCode, brightness,0));
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

// whatever documets are there, if these status from to any other status , just add a remove documet as response also to close the stream

    const changeStream = db.collection('draws').watch(
      [
        {
          $match: {
            $or: [
              { 'fullDocument.status': { $in: ACTIVE_DRAWN_STATUS } },
              { 'updateDescription.updatedFields.status': { $in: ACTIVE_DRAWN_STATUS } }
            ],
          },
        },
      ],
      { fullDocument: 'updateLookup' }
    );

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

      streamMetrics.changeEventsCount++;
      logger.log('Change event processing', {
        context: 'streamActiveDrawn',
        operationType: change.operationType,
        documentId: change.fullDocument._id,
        totalChanges: streamMetrics.changeEventsCount,
        timeSinceStart: Date.now() - streamMetrics.startTime,
      });

      subscriber.next(mapActiveDrawn(change.fullDocument, languageCode, brightness, 1));
    });

    changeStream.on('error', error => {
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


function mapActiveDrawn(doc: any, languageCode: string, brightness: string, streamType:number): ActiveDrawnResponse {
  return {
    id: doc._id?.toString() || '',
    contractId: doc.contractId || '',
    businessId: doc.businessId || '',
    type: doc.type || '',
    subtype: doc.subtype || '',
    currency: doc.currency || '',
    title: doc.title?.[languageCode] || doc.title?.en || 'Unknown Title',
    openAt: doc.openAt?.$date || doc.openAt || '',
    predrawStartAt: doc.predrawStartAt?.$date || doc.predrawStartAt || '',
    drawStartAt: doc.drawStartAt?.$date || doc.drawStartAt || '',
    contestsStartAt: doc.contestsStartAt?.$date || doc.contestsStartAt || '',
    descriptionFile: doc.descriptionFile?.[languageCode] || doc.descriptionFile?.en || 'No Description',
    logo: doc.logo?.[brightness]?.[languageCode] || doc.logo?.[brightness]?.en || '',
    amountOfNumbersByParticipant: doc.amountOfNumbersByParticipant ?? 0,
    grandDrawFreeTicketSpendingsAmount: doc.grandDrawFreeTicketSpendingsAmount ?? undefined,
    drawNumbersCount: doc.drawNumbersCount ?? 0,
    participantsCount: doc.participantsCount ?? 0,
    amountOfChosenNumbers: doc.amountOfChosenNumbers ?? 0,
    totalPrizesValue: doc.totalPrizesValue ?? 0,
    totalPrizesAmount: doc.totalPrizesAmount ?? 0,
    createdAt: doc.createdAt?.$date || doc.createdAt || '',
    status: doc.status || '',
    streamType: streamType
  };
}

