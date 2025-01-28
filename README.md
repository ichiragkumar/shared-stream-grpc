1) activeBusinessesStream - 
	Input - No
	Collection businesses
	Where - contractTypes in [‘vendor’,’advertiser','sponsor','specialIssue','business','voucher']

2) getBusinessBranchDetails
	Input - businessId, businessBranchId, languageCode
	Collection businessBranches,businessBranchDetails
	where - id=businessId, branch-id=businessBranchId and name, title depend on language

3) getBusinessById
	Input - String businessId, String languageCode, String brightness
	Collection businesses
	where id=businessId, name, title depend on language, image depend on brightness

4) getBusinessesWithIds
	Input - Array businessIds, String languageCode, String brightness
	Collection businesses
	where id in [businessIds], name, title depend on language, image depend on brightness
	
5) activeCouponIssuesStream
    input - No
    Collection couponIssues
    where status in ['active', 'suspended', 'ended']
 
6) activeCouponsStream
	input - userId
	Collection userCoupons
	where - userId=userId, status in ['active', 'suspended', 'ended'], redeemedBySelfActivation=false

7) activeMoreCouponsRequestsStream
	input - userId
	Collection moreCouponsRequests
	where- userId=userId

8) getCouponsByStatus
	input - userId,status
	Collection userCoupons
	where-    if (status == 'redeemed') {
      query = query
          .where(
            Filter.or(
              Filter('status', isEqualTo: 'redeemed'),
              Filter.and(Filter('status', isEqualTo: 'activated'),
                  Filter('redeemedBySelfActivation', isEqualTo: true)),
            ),
          )
          .where('redeemedAt', isNull: false)
          .orderBy('redeemedAt', descending: true);
    } else if (status == 'refunded') {
      query = query
          .where(Filter.or(
            Filter('status', isEqualTo: 'refunded'),
            Filter('status', isEqualTo: 'refundRequested'),
          ))
          .orderBy('purchasedAt', descending: true);
    } else {
      query = query
          .where('status', isEqualTo: status)
          .orderBy('purchasedAt', descending: true);
    }				
	

  	


9) activeCouponIssuesWithBusinesseStream
    input - No
    Collection couponIssues,businesses
    where status in ['active', 'suspended', 'ended']
    	 and couponIssues.businessId=businesses.id