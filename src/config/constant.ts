

export const WAE_DEFAULT_NAME = 'Unknown Name';
export const WAE_DEFAULT_LOGO = 'https://example.com/default-logo.png';
export const WAE_DEFAULT_CURRENCY = 'USD';

export const DEFAULT_COUPON_ISSUE_WITH_BUSINESS = {
  couponIssueId: '00001',
  businessId: '00001',
  couponName: WAE_DEFAULT_NAME,
  businessName: WAE_DEFAULT_NAME,
  status: 'unknown',
  logo: WAE_DEFAULT_LOGO,
  categories: [],
  endsAt: 'NOW',
  amountLeft: 0,
  type: 'unknown',
  priceAmount: 0,
  currency: WAE_DEFAULT_CURRENCY,
  drawId: '00001',
  sellPriceAmount: 0,
  restrictedBranchIds: [],
  drawNumbers: [],
  descriptionFile: '',
  purchasePriceAmount: 0,
  arrangement: 0,
  streamtype: 0
};


export const DEFAUlT_SETTINGS = {
  LANGUAGE_CODE: 'en',
  BRIGHTNESS: 'light'
};

export const VALID_CONTRACT_TYPES = ['vendor', 'advertiser', 'sponsor', 'specialIssue', 'business', 'voucher'];
export const NOT_TRACKED_CONTRACT_TYPES = ['closed', 'over', 'expired', 'unknown'];

export const BUSINESS_VALID_STATUS = ['active', 'suspended', 'ended'];
export const BUSINESS_NOT_TRACKED_STATUS = ['closed', 'expired', 'over'];


export const ACTIVE_DRAWN_STATUS = ['open', 'predraw', 'drawing', 'contest'];

export const USER_COUPON_STATUS = ['active', 'suspended', 'ended'];




// for coupon Issue Stream
export const TRACKED_STATUS = ['active', 'suspended', 'ended'];
export const NOT_TRACKED_STATUS = ['closed', 'expired', 'over'];
