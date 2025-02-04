import { Observable } from 'rxjs';
import { Db } from 'mongodb';
import {
  MoreCouponRequest,
  User,
} from '../../generated/coupon_stream';

export function streamMoreCouponRequestsService(
  db: Db,
  data: User,
): Observable<MoreCouponRequest> {
  return new Observable<MoreCouponRequest>((subscriber) => {
    const { userId } = data;

    (async () => {
      try {
        const userExists = await db
          .collection('moreCouponsRequests')
          .findOne({ userId });

        if (!userExists) {
          subscriber.next({
            id: '',
            userId,
            couponIssueId: '',
            createdAt: 0,
            parentId: '',
          });
          subscriber.complete();
          return;
        }

        const initialDocuments = db
          .collection('moreCouponsRequests')
          .find({ userId });
        for await (const document of initialDocuments) {
          subscriber.next({
            id: document._id.toString(),
            userId: document.userId,
            couponIssueId: document.couponIssueId,
            createdAt: new Date(document.createdAt).getTime(),
            parentId: document.parentId || '',
          });
        }

        db.collection('moreCouponsRequests')
          .findOne({ userId })
          .then((userExists) => {
            const changeStream = db.collection('moreCouponsRequests').watch(
              [
                {
                  $match: {
                    'fullDocument.userId': userId,
                  },
                },
              ],
              { fullDocument: 'updateLookup' },
            );

            changeStream.on('change', (change: any) => {
              if (change.fullDocument) {
                const { _id, userId, couponIssueId, createdAt, _parentId } =
                  change.fullDocument;

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
              subscriber.error(
                new Error('An error occurred while streaming changes.'),
              );
            });

            return () => {
              console.log('Cleaning up change stream');
              changeStream.close();
            };
          })
          .catch((err) => {
            subscriber.error(err);
          });
      } catch (error) {
        console.error('Error in streaming:', error);
        subscriber.error(error);
      }
    })();
  });
}
