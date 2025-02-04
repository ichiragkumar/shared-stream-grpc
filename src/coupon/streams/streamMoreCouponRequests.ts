import { Observable } from 'rxjs';
import { Db } from 'mongodb';
import { MoreCouponRequest, User, UserFilter } from '../../generated/coupon_stream';

export function streamMoreCouponRequestsService(data: User, db: Db): Observable<MoreCouponRequest> {
  return new Observable<MoreCouponRequest>(subscriber => {
    const { userId } = data;

    if (!userId) {
      subscriber.error(new Error('Invalid request: userId is required.'));
      return;
    }

    db.collection('moreCouponsRequests').findOne({ userId })
      .then(userExists => {
        if (!userExists) {
          subscriber.error(new Error(`No more coupon requests found for the given userId: ${userId}`));
          return;
        }

        const changeStream = db.collection('moreCouponsRequests').watch(
          [
            {
              $match: {
                'fullDocument.userId': userId,
              },
            },
          ],
          { fullDocument: 'updateLookup' }
        );

        changeStream.on('change', (change: any) => {
          if (change.fullDocument) {
            const { _id, userId, couponIssueId, createdAt, _parentId } = change.fullDocument;

            const moreCouponRequest: MoreCouponRequest = {
              id: _id.toString(),
              userId,
              couponIssueId,
              createdAt: new Date(createdAt).getTime(),
              parentId: _parentId,
            };

            subscriber.next(moreCouponRequest);
          }
        });

        changeStream.on('error', (err: any) => {
          console.error('Change stream error:', err);
          subscriber.error(new Error('An error occurred while streaming changes.'));
        });

        return () => {
          console.log('Cleaning up change stream');
          changeStream.close();
        };
      })
      .catch(err => {
        subscriber.error(err);
      });
  });
}
