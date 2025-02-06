import { Observable } from 'rxjs';
import { Db } from 'mongodb';
import { ActiveDrawnResponse, UserPrefrences } from 'src/generated/coupon_stream';
import { ACTIVE_DRAWN_STATUS, DEFAUlT_SETTINGS } from 'src/config/constant';

export function streamActiveDrawn(db: Db, languageFilter: UserPrefrences): Observable<ActiveDrawnResponse> {
  return new Observable(subscriber => {
    const languageCode = languageFilter?.languageCode || DEFAUlT_SETTINGS.LANGUAGE_CODE;
    const brightness = languageFilter?.brightness || DEFAUlT_SETTINGS.BRIGHTNESS;

    (async () => {
      try {
        const initialDocuments = db.collection('draws').find({ status: { $in: ACTIVE_DRAWN_STATUS } });

        for await (const document of initialDocuments) {
          subscriber.next(mapActiveDrawn(document, languageCode, brightness));
        }
      } catch (error) {
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

    changeStream.on('change', (change: any) => {
      if (!change.fullDocument) return;
      subscriber.next(mapActiveDrawn(change.fullDocument, languageCode, brightness));
    });

    changeStream.on('error', error => {
      console.error('Change stream error:', error);
      subscriber.error(error);
    });

    subscriber.add(() => {
      console.log('Cleaning up change stream');
      changeStream.close();
    });
  });
}


function mapActiveDrawn(doc: any, languageCode: string, brightness: string): ActiveDrawnResponse {
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
  };
}

