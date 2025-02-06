import { Observable } from 'rxjs';
import { Db } from 'mongodb';
import { CouponIssueWithBusiness, UserPrefrences } from 'src/generated/coupon_stream';
import { safeParseDate, STREAM_TYPE } from 'src/types';
import { DEFAULT_COUPON_ISSUE_WITH_BUSINESS, DEFAUlT_SETTINGS } from 'src/config/constant';

const VALID_STATUS  = ['active', 'suspended', 'ended'];
const BUSINESS_NOT_TRACKED_STATUS = ['closed', 'expired', 'over'];

const mapToCouponIssue = (
  document: any, 
  lang: string, 
  bright: string, 
  streamType: STREAM_TYPE
): CouponIssueWithBusiness => {
  return {
    couponIssueId: document._id?.toString() || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.couponIssueId,
    businessId: document.businessId?.toString() || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.businessId,
    couponName: document.title?.[lang] || document.title?.en || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.couponName,
    businessName: document.business?.title?.[lang] || document.business?.title?.en || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.businessName,
    status: document.status || DEFAULT_COUPON_ISSUE_WITH_BUSINESS.status,
    logo: document.business?.logo?.[bright]?.[lang] || 
          document.business?.logo?.[bright]?.en || 
          document.business?.logo?.dark?.[lang] || 
          document.business?.logo?.dark?.en || 
          DEFAULT_COUPON_ISSUE_WITH_BUSINESS.logo,
    categories: Array.isArray(document.business?.categories) ? document.business.categories : [],
    endsAt: document.endAt ? safeParseDate(document.endAt) : DEFAULT_COUPON_ISSUE_WITH_BUSINESS.endsAt as any,
    amountLeft: typeof document.initialAmount === 'number' ? Math.max(0, document.initialAmount - (document.amountSold || 0)) : DEFAULT_COUPON_ISSUE_WITH_BUSINESS.amountLeft,
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
    const languageCode = languageFilter?.languageCode || DEFAUlT_SETTINGS.LANGUAGE_CODE;
    const brightness = languageFilter?.brightness || DEFAUlT_SETTINGS.BRIGHTNESS;

    const pipeline = [
      { $match: { status: { $in: VALID_STATUS } } },
      {
        $lookup: {
          from: 'businesses',
          localField: 'businessId',
          foreignField: '_id',
          as: 'business'
        }
      },
      { $unwind: { path: '$business', preserveNullAndEmptyArrays: true } }
    ];

    (async () => {
      try {
        const initialResults = await db.collection('couponIssues').aggregate(pipeline).toArray();
        for (const document of initialResults) {
          subscriber.next(mapToCouponIssue(document, languageCode, brightness, STREAM_TYPE.BASE));
        }

        const changeStream = db.collection('couponIssues').watch([], { fullDocument: 'updateLookup' });

        changeStream.on('change', async (change: any) => {
          try {
            let streamType: STREAM_TYPE;
            let mappedIssue: CouponIssueWithBusiness | null = null;

            const previousStatus = change?.updateDescription?.updatedFields?.status ?? change?.fullDocumentBeforeChange?.status;
            const newStatus = change.fullDocument?.status;
            if(BUSINESS_NOT_TRACKED_STATUS.includes(newStatus)){
              streamType = STREAM_TYPE.REMOVED;
              mappedIssue = mapToCouponIssue(change.fullDocument, languageCode, brightness, streamType);
            }
            const wasTracked = previousStatus && VALID_STATUS.includes(previousStatus);
            const isNowTracked = VALID_STATUS.includes(newStatus);


            if (wasTracked && isNowTracked) {
              streamType = STREAM_TYPE.UPDATE;
              mappedIssue = mapToCouponIssue(change.fullDocument, languageCode, brightness, streamType);
            } else if (!wasTracked && isNowTracked) {
              streamType = STREAM_TYPE.INSERT;
              mappedIssue = mapToCouponIssue(change.fullDocument, languageCode, brightness, streamType);
            }

            if (mappedIssue) {
              subscriber.next(mappedIssue);
            }
          } catch (error) {
            console.error('Error processing change stream event:', error);
            subscriber.error(error);
          }
        });

        changeStream.on('error', (error) => {
          console.error('Change stream error:', error);
          subscriber.error(error);
        });

        subscriber.add(() => {
          console.log('Cleaning up change stream');
          changeStream.close();
        });
      } catch (error) {
        console.error('Stream initialization error:', error);
        subscriber.error(error);
      }
    })();
  });
}
