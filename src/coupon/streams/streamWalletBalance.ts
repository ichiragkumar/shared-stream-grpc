import { Observable } from 'rxjs';
import { Db } from 'mongodb';
import { Balance, UserFilter } from 'src/generated/coupon_stream';

export function streamWalletBalance(db: Db, data: UserFilter): Observable<Balance> {
  return new Observable(subscriber => {
    db.collection('wallets').findOne({ 'userId': data.userId })
      .then((doc) => {
        if (doc && doc.availableBalances) {
          const availableBalances = doc.availableBalances || {};
          const initialBalance: Balance = {
            USD: availableBalances.USD || 0,
            EGP: availableBalances.EGP || 0
          };
          subscriber.next(initialBalance);
        } else {
          subscriber.next({ USD: 0, EGP: 0 });
        }
      })
      .catch((error) => {
        console.error('Error fetching initial wallet balance:', error);
        subscriber.error(error);
      });


    const changeStream = db.collection('wallets').watch(
      [
        { $match: { 'fullDocument.userId': data.userId } }
      ],
      { fullDocument: 'updateLookup' }
    );

    changeStream.on('change', (change: any) => {
      if (change.fullDocument) {
        const availableBalances = change.fullDocument.availableBalances || {};
        const updatedBalance: Balance = {
          USD: availableBalances.USD || 0,
          EGP: availableBalances.EGP || 0
        };
        subscriber.next(updatedBalance);
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
