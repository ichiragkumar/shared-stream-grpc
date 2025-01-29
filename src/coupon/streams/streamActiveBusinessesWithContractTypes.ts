import { Observable } from 'rxjs';
import { Db } from 'mongodb';
import { ActiveBusinessesStreamResponse } from '../../generated/coupon_stream';

export function streamActiveBusinessesWithContractTypes(db: Db): Observable<ActiveBusinessesStreamResponse> {
  return new Observable(subscriber => {
    const changeStream = db.collection('businesses').watch(
      [
        {
          $match: {
            'fullDocument.contractTypes': { $in: ['vendor', 'advertiser', 'sponsor', 'specialIssue', 'business', 'voucher'] },
          },
        },
      ],
      { fullDocument: 'updateLookup' }
    );

    changeStream.on('change', async (change: any) => {
      if (change.fullDocument) {
        const business = change.fullDocument;

        const response: ActiveBusinessesStreamResponse = {
          id: business._id.toString(),
          title: business.title?.en || 'Unknown Title',
          description: business.description?.en || 'No Description',
          image: business.logo?.light?.en || null,
          categories: business.categories || [],
          businessId: business._id.toString(),
          contractType: business.contractTypes.join(', '),
        };

        subscriber.next(response);
      }
    });

    changeStream.on('error', (error: any) => {
      console.error('Change stream error:', error);
      subscriber.error(error);
    });

    return () => {
      console.log('Cleaning up change stream');
      changeStream.close();
    };
  });
}
