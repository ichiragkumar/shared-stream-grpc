import { Observable } from 'rxjs';
import { Db, ObjectId } from 'mongodb';
import { CouponIssueWithBusiness, LanguageFilter, UserFilter } from 'src/generated/coupon_stream';
import { PAGE_LIMIT, safeParseDate } from 'src/types';
import { DEFAULT_COUPON_ISSUE_WITH_BUSINESS } from 'src/config/constant';

const mapToCouponIssue = (doc: any, languageCode: string): CouponIssueWithBusiness => {
  const lang = languageCode || 'en';
  
  return {
    couponIssueId: doc._id?.toString() || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.couponIssueId,
    businessId: doc.businessId?.toString() || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.businessId,
    couponName: doc.title?.[lang] || doc.title?.en || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.couponName,
    businessName: doc.business?.title?.[lang] || doc.business?.title?.en || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.businessName,
    status: doc.status || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.status,
    logo: doc.business?.logo?.light?.[lang] || 
          doc.business?.logo?.light?.en || 
          doc.business?.logo?.dark?.[lang] || 
          doc.business?.logo?.dark?.en || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.logo,
    categories: Array.isArray(doc.business?.categories) ? doc.business.categories : [],
    endsAt: doc.endAt ? safeParseDate(doc.endAt) : DEFAULT_COUPON_ISSUE_WITH_BUSINESS.endsAt,
    amountLeft: typeof doc.initialAmount === 'number' ? doc.initialAmount - (doc.amountSold || 0) : DEFAULT_COUPON_ISSUE_WITH_BUSINESS.amountLeft,
    type: doc.type || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.type,
    priceAmount: Number(doc.priceAmount) || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.priceAmount,
    currency: doc.currency || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.currency,
    drawId: doc.drawId || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.drawId,
    sellPriceAmount: Number(doc.sellPriceAmount) || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.sellPriceAmount,
    restrictedBranchIds: Array.isArray(doc.zoneIds) ? doc.zoneIds : DEFAULT_COUPON_ISSUE_WITH_BUSINESS.restrictedBranchIds,
    drawNumbers: Array.isArray(doc.drawNumbers) ? doc.drawNumbers : DEFAULT_COUPON_ISSUE_WITH_BUSINESS.drawNumbers,
    descriptionFile: doc.descriptionFile?.[lang] || doc.descriptionFile?.en || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.descriptionFile,
    purchasePriceAmount: Number(doc.purchasePriceAmount) || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.purchasePriceAmount,
    arrangement: Number(doc.arrangement) || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.arrangement
  };
};

export function streamActiveCouponIssuesWithBusiness(db: Db, languageFilter: LanguageFilter): Observable<CouponIssueWithBusiness> {
  return new Observable(subscriber => {
    (async () => {
      try {
        const languageCode = languageFilter?.languageCode || 'ar';
        
        const pipeline = [
          { 
            $match: { 
              status: { $in: ['active', 'suspended', 'ended'] }
            } 
          },
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

        for (const doc of initialResults) {
          const couponIssue = mapToCouponIssue(doc, languageCode);
          subscriber.next(couponIssue);
        }


        const changeStream = db.collection('couponIssues').watch(
          [{ 
            $match: { 
              $or: [
                { 'operationType': 'insert' },
                { 'operationType': 'update' },
                { 'operationType': 'replace' },
                { 'operationType': 'delete' },
                {
                  'operationType': 'update',
                  'updateDescription.updatedFields.status': { 
                    $exists: true 
                  }
                }
              ]
            } 
          }],
          { fullDocument: 'updateLookup' }
        );

        changeStream.on('change', async (change: any) => {
          try {
           if (change.fullDocument) {
              const updatedDoc = await db.collection('couponIssues')
                .aggregate([
                  { 
                    $match: { 
                      _id: change.fullDocument._id
                    } 
                  },
                  ...pipeline
                ])
                .next();

              if (updatedDoc) {
                const couponIssue = mapToCouponIssue(updatedDoc, languageCode);
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