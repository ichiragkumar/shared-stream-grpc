import { Observable } from 'rxjs';
import { Db } from 'mongodb';
import { UserFilter, ActiveCouponStreamResponse, StatusFilter, User } from '../../generated/coupon_stream';

export function streamActiveCouponsStream(db: Db, data: User): Observable<ActiveCouponStreamResponse> {
  return new Observable(subscriber => {

    const { userId } = data;

    db.collection('userCoupons').findOne({ userId })
      .then(userExists => {
        if (!userExists) {
          console.log('user does not exist');
          subscriber.error(new Error('user does not exist'));

          return;
        }


        const changeStream = db.collection('userCoupons').watch(
          [
            {
              $match: {
                'fullDocument.userId': userId,
              },
            },
          ],
          { fullDocument: 'updateLookup' }
        );


        changeStream.on('change', async (change: any) => {
          if (change.fullDocument) {
            if (!change.fullDocument._id || !change.fullDocument.userId) {
              subscriber.complete(); 
              return;
            }


            const couponIssue: any = {
              _id: change.fullDocument._id.toString(),
              redemptionInfo: change.fullDocument.redemptionInfo || null,
              code: change.fullDocument.code,
              businessId: change.fullDocument.businessId,
              couponIssueId: change.fullDocument._id.toString(),
              redeemedBySelfActivation: change.fullDocument.redeemedBySelfActivation,
              purchasePrice: change.fullDocument.purchasePrice,
              purchaseCurrency: change.fullDocument.purchaseCurrency,
              userId: change.fullDocument.userId,
              status: change.fullDocument.status,
              expireAt: { seconds: change.fullDocument.expireAt.getTime() / 1000, nanos: 0 }, 
              createdAt: { seconds: change.fullDocument.createdAt.getTime() / 1000, nanos: 0 },
              purchasedAt: { seconds: change.fullDocument.purchasedAt.getTime() / 1000, nanos: 0 }
            };
            subscriber.next(couponIssue);
          }
        });


        changeStream.on('error', (error: any) => {

          subscriber.complete(); 
        });

        return () => {
          console.log('Cleaning up change stream');
          changeStream.close();
        };
      })
      .catch(error => {

        subscriber.complete(); 
      });
  });
}
