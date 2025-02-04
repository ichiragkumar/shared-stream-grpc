import { Observable } from 'rxjs';
import { Db } from 'mongodb';
import { Balance, WalletBalanceResponse, User } from 'src/generated/coupon_stream';
import { STREAM_TYPE } from 'src/types';

export function streamWalletBalance(db: Db, data: User): Observable<WalletBalanceResponse> {
  return new Observable<WalletBalanceResponse>(subscriber => {
    const { userId } = data;
    (async () => {
      try {
        const walletDoc = await db.collection('wallets').findOne({ userId });

        if (!walletDoc) {
          subscriber.next({
            availableBalances: { USD: 0, EGP: 0 },
            blockedBalances: { USD: 0, EGP: 0 },
            type: STREAM_TYPE.BASE,
          });
        } else {
          const availableBalances: Balance = walletDoc.availableBalances || { USD: 0, EGP: 0 };
          const blockedBalances: Balance = walletDoc.blockedBalances || { USD: 0, EGP: 0 };

          subscriber.next({
            availableBalances,
            blockedBalances,
            type: STREAM_TYPE.BASE,
          });
        }

        const changeStream = db.collection('wallets').watch(
          [{ $match: { 'fullDocument.userId': userId } }],
          { fullDocument: 'updateLookup' }
        );

        changeStream.on('change', (change: any) => {
          if (!change.fullDocument) return;

          let streamType: STREAM_TYPE = STREAM_TYPE.BASE;
          switch (change.operationType) {
            case 'insert':
              streamType = STREAM_TYPE.INSERT;
              break;
            case 'update':
              streamType = STREAM_TYPE.UPDATE;
              break;
            default:
              return;
          }

          const availableBalances = change.fullDocument.availableBalances || { USD: 0, EGP: 0 };
          const blockedBalances = change.fullDocument.blockedBalances || { USD: 0, EGP: 0 };

          subscriber.next({ availableBalances, blockedBalances, type: streamType });
        });

        changeStream.on('error', (error: any) => {
          console.error('Change stream error:', error);
          subscriber.error(new Error('An error occurred while streaming wallet balance updates.'));
        });

        subscriber.add(() => {
          console.log('Cleaning up wallet balance change stream');
          changeStream.close();
        });
      } catch (error) {
        console.error('Error streaming wallet balance:', error);
        subscriber.error(error);
      }
    })();
  });
}
