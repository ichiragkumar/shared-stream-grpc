import { Observable } from 'rxjs';
import { Db } from 'mongodb';
import { Balance, UserFilter } from 'src/generated/coupon_stream';
import {  STREAM_TYPE } from 'src/types';



export function streamWalletBalance(db: Db, data: UserFilter): Observable<Balance> {
  return new Observable(subscriber => {
    db.collection('wallets').findOne({ userId: data.userId })
      .then((doc) => {
        const availableBalances: Balance = doc?.availableBalances || { USD: 0, EGP: 0 };
        subscriber.next({ ...availableBalances, type: STREAM_TYPE.BASE });
      })
      .catch((error) => {
        console.error('Error fetching initial wallet balance:', error);
        subscriber.error(error);
      });
    const changeStream = db.collection('wallets').watch(
      [{ $match: { 'fullDocument.userId': data.userId } }],
      { fullDocument: 'updateLookup' }
    );

    changeStream.on('change', (change: any) => {
      let streamType: STREAM_TYPE;
      let balanceData: Balance = { USD: 0, EGP: 0 , type: STREAM_TYPE.BASE };

      switch (change.operationType) {
        case 'insert':
          streamType = STREAM_TYPE.INSERT;
          balanceData = change.fullDocument.availableBalances || { USD: 0, EGP: 0 };
          break;
        case 'update':
          streamType = STREAM_TYPE.UPDATE;
          balanceData = change.fullDocument.availableBalances || { USD: 0, EGP: 0 };
          break;
        default:
          return;
      }


      subscriber.next({ ...balanceData, type: streamType });
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
