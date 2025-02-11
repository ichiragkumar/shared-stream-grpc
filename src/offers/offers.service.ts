import { BadRequestException, Injectable } from '@nestjs/common';
import { Db, Filter } from 'mongodb';
import { Language } from 'src/config/constant';
import { DatabaseService } from 'src/config/database.config';
import { BusinessDocument, SpecialOfferDocument } from 'src/types';

@Injectable()
export class OffersService {


     private db: Db;
    
      constructor() {}
      async onModuleInit() {
        this.db = await DatabaseService.connect();
      }

      

    async getSpecialOffers(ids: string[], languageCode: string, brightness: string) {
        if (!ids || ids.length === 0) {
            throw new BadRequestException("'ids' must be a non-empty array. : getBusinessesWithIds");
        }
    try {
       const filter: Filter<SpecialOfferDocument> = { 
               _id: { $in: ids } 
        } as Filter<SpecialOfferDocument>;

        const specialOffersDocument = await this.db.collection<SpecialOfferDocument>('specialOffers').find(filter).toArray();
        if(!specialOffersDocument){
            return {
                msg:"No special offers found for the provided IDs."
            }
        }
        const businessIds = specialOffersDocument.map(offer => offer.businessId);


        const businesses = await this.db.collection<BusinessDocument>('businesses').find({ _id: { $in: businessIds } }).toArray();
        if(!businesses || businesses.length==0){
            return {
                msg:"No businesses found for the provided IDs."
            }
        }

        const businessDocumentMap = new Map(
            businesses.map(business => [
              business._id.toString(),
              {
                title: business.title?.[languageCode] || business.title?.[Language.DEFAULT] || 'Unknown',
                categories: business.categories || 'other',
              },
            ])
          );
      

        return specialOffersDocument.map(offer => ({
            businessTitle: businessDocumentMap.get(offer.businessId)?.title || 'Unknown',
            categories: businessDocumentMap.get(offer.businessId)?.categories || 'other',
            businessId: offer.businessId,
            offerTitle: offer.title?.[languageCode] || offer.title?.[Language.DEFAULT] || 'Unknown',
            content: offer.content?.[languageCode] || offer.content?.[Language.DEFAULT] || 'No content available',
            displayedFrom: offer.displayedFrom,
            displayedUntil: offer.displayedUntil,
            image: offer.image?.[brightness] || null,
          }));
        } catch (error) {
            return {
                message: `Internal Server Error :getBusinessesWithIds`,
            }
        }

    }
}
