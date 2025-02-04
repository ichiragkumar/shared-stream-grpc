import { Observable } from 'rxjs';
import { Db } from 'mongodb';
import { Balance, WalletBalanceResponse, UserFilter, User } from 'src/generated/coupon_stream';
import { STREAM_TYPE } from 'src/types';

export function streamWalletBalance(db: Db, data:User): Observable<WalletBalanceResponse> {
  return new Observable(subscriber => {
    db.collection('wallets').findOne({ userId: data.userId })
      .then((doc) => {
        const availableBalances: Balance = doc?.availableBalances || { USD: 0, EGP: 0 };
        const blockedBalances: Balance = doc?.blockedBalances || { USD: 0, EGP: 0 };

        

        subscriber.next({ availableBalances, blockedBalances, type: STREAM_TYPE.BASE });
      })
      .catch((error) => {
        console.error('Not able to find userId', error);
        subscriber.error(error);
      });

    const changeStream = db.collection('wallets').watch(
      [{ $match: { 'fullDocument.userId': data.userId } }],
      { fullDocument: 'updateLookup' }
    );

    changeStream.on('change', (change: any) => {
      let streamType: STREAM_TYPE = STREAM_TYPE.BASE;
      let availableBalances: Balance = { USD: 0, EGP: 0 };
      let blockedBalances: Balance = { USD: 0, EGP: 0 };

      switch (change.operationType) {
        case 'insert':
        case 'update':
          streamType = change.operationType === 'insert' ? STREAM_TYPE.INSERT : STREAM_TYPE.UPDATE;
          availableBalances = change.fullDocument?.availableBalances || { USD: 0, EGP: 0 };
          blockedBalances = change.fullDocument?.blockedBalances || { USD: 0, EGP: 0 };
          break;
        default:
          return;
      }

      subscriber.next({ availableBalances, blockedBalances, type: streamType });
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
