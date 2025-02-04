import { Observable } from 'rxjs';
import { Db } from 'mongodb';
import { CouponIssueWithBusiness, UserPrefrences } from 'src/generated/coupon_stream';
import { safeParseDate, STREAM_TYPE } from 'src/types';
import { DEFAULT_COUPON_ISSUE_WITH_BUSINESS } from 'src/config/constant';

let streamType: STREAM_TYPE;

const mapToCouponIssue = (document: any, languageCode: string): CouponIssueWithBusiness => {
  const lang = languageCode || 'en';
  return {
    couponIssueId: document._id?.toString() || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.couponIssueId,
    businessId: document.businessId?.toString() || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.businessId,
    couponName: document.title?.[lang] || document.title?.en || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.couponName,
    businessName: document.business?.title?.[lang] || document.business?.title?.en || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.businessName,
    status: document.status || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.status,
    logo: document.business?.logo?.light?.[lang] || 
          document.business?.logo?.light?.en || 
          document.business?.logo?.dark?.[lang] || 
          document.business?.logo?.dark?.en || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.logo,
    categories: Array.isArray(document.business?.categories) ? document.business.categories : [],
    endsAt: document.endAt ? safeParseDate(document.endAt) : DEFAULT_COUPON_ISSUE_WITH_BUSINESS.endsAt as any,
    amountLeft: typeof document.initialAmount === 'number' ? document.initialAmount - (document.amountSold || 0) : DEFAULT_COUPON_ISSUE_WITH_BUSINESS.amountLeft,
    type: document.type || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.type,
    priceAmount: Number(document.priceAmount) || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.priceAmount,
    currency: document.currency || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.currency,
    drawId: document.drawId || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.drawId,
    sellPriceAmount: Number(document.sellPriceAmount) || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.sellPriceAmount,
    restrictedBranchIds: Array.isArray(document.zoneIds) ? document.zoneIds : DEFAULT_COUPON_ISSUE_WITH_BUSINESS.restrictedBranchIds,
    drawNumbers: Array.isArray(document.drawNumbers) ? document.drawNumbers : DEFAULT_COUPON_ISSUE_WITH_BUSINESS.drawNumbers,
    descriptionFile: document.descriptionFile?.[lang] || document.descriptionFile?.en || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.descriptionFile,
    purchasePriceAmount: Number(document.purchasePriceAmount) || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.purchasePriceAmount,
    arrangement: Number(document.arrangement) || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.arrangement,
    streamtype: streamType || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.streamtype
  };
};

export function streamActiveCouponIssuesWithBusiness(db: Db, languageFilter: UserPrefrences): Observable<CouponIssueWithBusiness> {
  return new Observable(subscriber => {
    (async () => {
      try {
        const languageCode = languageFilter?.languageCode || 'ar';

        const pipeline = [
          { 
            $lookup: {
              from: 'businesses',
              localField: 'businessId',
              foreignField: '_id',
              as: 'business'
            }
          },
          { 
            $unwind: { 
              path: '$business', 
              preserveNullAndEmptyArrays: true 
            } 
          }
        ];


        const initialResults = await db.collection('couponIssues')
          .aggregate(pipeline)
          .toArray();

        for (const document of initialResults) {
          const couponIssue = mapToCouponIssue(document, languageCode);
          subscriber.next(couponIssue);
        }


        const changeStream = db.collection('couponIssues').watch(
          [{ $match: {} }],
          { fullDocument: 'updateLookup' }
        );

        changeStream.on('change', async (change: any) => {
          try {
            switch (change.operationType) {
              case 'insert':
                streamType = STREAM_TYPE.INSERT;
                break;
              case 'update':
              case 'replace':
                streamType = STREAM_TYPE.UPDATE;
                break;
              case 'delete':
                streamType = STREAM_TYPE.DELETE;
                break;
              default:
                return;
            }

            if (change.fullDocument) {
              const updatedDocument = await db.collection('couponIssues')
                .aggregate([
                  { $match: { _id: change.fullDocument._id } },
                  ...pipeline
                ])
                .next();

              if (updatedDocument) {
                const couponIssue = mapToCouponIssue(updatedDocument, languageCode);
                subscriber.next(couponIssue);
              }
            }
          } catch (error) {
            subscriber.error(error);
          }
        });

        changeStream.on('error', (error) => {
          subscriber.error(error);
        });

        return () => {
          changeStream.close();
        };
      } catch (error) {
        console.error('Stream initialization error:', error);
        subscriber.error(error);
      }
    })();
  });
}
