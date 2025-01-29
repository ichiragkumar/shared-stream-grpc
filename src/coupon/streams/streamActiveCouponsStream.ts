import { Observable } from 'rxjs';
import { Db } from 'mongodb';
import { ActiveCouponStreamResponse, UserFilter } from '../../generated/coupon_stream';


export function streamActiveCouponsStream(db: Db, data: UserFilter): Observable<ActiveCouponStreamResponse> {
  return new Observable(subscriber => {
    if (!data || !data.userId) {
      subscriber.error(new Error('Invalid request: userId is required.'));
      return;
    }

    const { userId } = data;

    db.collection('userCoupons').findOne({ userId })
      .then(userExists => {
        if (!userExists) {
          subscriber.error(new Error(`No coupon requests found for userId: ${userId}`));
          return;
        }

        const changeStream = db.collection('userCoupons').watch(
          [
            {
              $match: {
                'fullDocument.status': { $in: ['active', 'suspended', 'ended'] },
                'fullDocument.userId': userId
              },
            },
          ],
          { fullDocument: 'updateLookup' }
        );

        changeStream.on('change', async (change: any) => {
          if (change.fullDocument) {
            if (!change.fullDocument._id || !change.fullDocument.userId) {
              subscriber.error(new Error('Invalid document data received'));
              return;
            }

            const couponIssue: ActiveCouponStreamResponse = {
              _id: change.fullDocument._id,
              redemptionInfo: change.fullDocument.redemptionInfo || null,
              code: change.fullDocument.code,
              businessId: change.fullDocument.businessId,
              couponIssueId: change.fullDocument._id.toString(),
              redeemedBySelfActivation: change.fullDocument.redeemedBySelfActivation,
              purchasePrice: change.fullDocument.purchasePrice,
              purchaseCurrency: change.fullDocument.purchaseCurrency,
              userId: change.fullDocument.userId,
              status: change.fullDocument.status,
              expireAt: change.fullDocument.expireAt,
              createdAt: change.fullDocument.createdAt,
              purchasedAt: change.fullDocument.purchasedAt,
            };

            subscriber.next(couponIssue);
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
      })
      .catch(error => {
        subscriber.error(error);
      });
  });
}
