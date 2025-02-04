import { Observable } from 'rxjs';
import { Db } from 'mongodb';
import { ActiveDrawnResponse, UserPrefrences } from 'src/generated/coupon_stream';

export function streamActiveDrawn(db: Db, languageFilter: UserPrefrences): Observable<ActiveDrawnResponse> {
  return new Observable(subscriber => {
    const languageCode = languageFilter?.languageCode || 'en';
    const validStatuses = ['open', 'predraw', 'drawing', 'contest'];


    (async () => {
      try {
        const initialDocuments = db.collection('draws').find({ status: { $in: validStatuses } });
        for await (const document of initialDocuments) {
          subscriber.next(mapActiveDrawn(document, languageCode));
        }
      } catch (error) {
        console.error('Error fetching initial documents:', error);
        subscriber.error(error);
      }
    })();

    const changeStream = db.collection('draws').watch(
      [
        {
          $match: {
            'fullDocument.status': { $in: validStatuses }
          },
        },
      ],
      { fullDocument: 'updateLookup' }
    );

    changeStream.on('change', (change: any) => {
      if (!change.fullDocument) return;
      subscriber.next(mapActiveDrawn(change.fullDocument, languageCode));
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

function mapActiveDrawn(doc: any, languageCode: string): ActiveDrawnResponse {
  return {
    id: doc._id?.toString() || '',
    contractId: doc.contractId || '',
    businessId: doc.businessId || '',
    type: doc.type || '',
    subtype: doc.subtype || '',
    currency: doc.currency || '',
    title: doc.title?.[languageCode] || doc.title?.en || 'Unknown Title',
    openAt: doc.openAt || '',
    predrawStartAt: doc.predrawStartAt || '',
    drawStartAt: doc.drawStartAt || '',
    contestsStartAt: doc.contestsStartAt || '',
    descriptionFile: doc.descriptionFile?.[languageCode] || doc.descriptionFile?.en || 'No Description',
    logo: doc.logo?.dark?.[languageCode] || doc.logo?.light?.[languageCode] || doc.logo?.dark?.en || doc.logo?.light?.en || '',
    amountOfNumbersByParticipant: doc.amountOfNumbersByParticipant ?? 0,
    grandDrawFreeTicketSpendingsAmount: doc.grandDrawFreeTicketSpendingsAmount ?? undefined,
    drawNumbersCount: doc.drawNumbersCount ?? 0,
    participantsCount: doc.participantsCount ?? 0,
    createdAt: doc.createdAt || '',
    amountOfChosenNumbers: doc.amountOfChosenNumbers ?? 0,
    totalPrizesValue: doc.totalPrizesValue ?? 0,
    totalPrizesAmount: doc.totalPrizesAmount ?? 0,
    status: doc.status || '',
  };
}
