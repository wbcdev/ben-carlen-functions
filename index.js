// deploy with this, in this (functions) directory
// firebase deploy --only functions:syncMlsData
const { onSchedule } = require("firebase-functions/v2/scheduler")
const admin = require('firebase-admin');
const moment = require('moment');
const fetch = require('node-fetch');

const { google } = require('googleapis')

if (!admin.apps.length) { admin.initializeApp(); }
const db = admin.firestore();

// --- YOUR EXACT REUSED LOGIC ---
const computePropertyLabels = (property) => {
    if (!property) return null;
  
    let imgArray = []
    if (property.Media && Array.isArray(property.Media)) {
      property.Media.forEach(element => {
        if (element && element.MediaURL) {
          imgArray.push(element.MediaURL)
        }
      })
    }
  
    let saleOrRentArray = []
    let labelArray = []
    let propertyTypeArray = []
    let contractStatusLabelArray = []
  
    let A = moment().startOf('day')
    let B = moment(property.ListingContractDate || property.OriginalEntryTimestamp).startOf('day')
    let diffDays = A.diff(B, 'days')
    
    let reduced = (property.OriginalListPrice && property.ListPrice) ? property.OriginalListPrice / property.ListPrice : 1
  
    switch (true) {
      case property.OpenHouseStatus == 'Active':
        contractStatusLabelArray.push('open house')
        break
      default:
        contractStatusLabelArray.push('')
    }
  
    const isBackOnMarket = 
    property.MlsStatus === 'Active' && 
    property.ContingentDate && 
    moment(property.ModificationTimestamp).isAfter(moment(property.ContingentDate));
  
    switch (true) {
      case property.MlsStatus == 'Sold':
        contractStatusLabelArray.push('sold')
        break
      case isBackOnMarket:
        contractStatusLabelArray.push('back on market')
        break
      case property.NAV17_rets_status == 'Active Under Contract':
        contractStatusLabelArray.push('contingent')
        break
      case (property.OriginalListPrice != null && reduced != 1 && property.OriginalListPrice > property.ListPrice):
        contractStatusLabelArray.push('reduced')
        break
      case diffDays >= 0 && diffDays <= 10:
        contractStatusLabelArray.push('just listed')
        break
      default:
        contractStatusLabelArray.push('')
    }
    
    const subType = (property.PropertySubType || "").trim();
  
    switch(property.PropertyType) {
      case "Residential": 
        if (subType === "Residential Auction") {
          propertyTypeArray.push('Residential Auction');
        } else if (subType === "Residential Rental/Property Management") {
          propertyTypeArray.push('Residential Lease');
        } else {
          propertyTypeArray.push('Single Family Home');
        }
        break;
  
      case "Land": 
        if (subType === "Land Auction") {
          propertyTypeArray.push('Land Auction');
        } else {
          propertyTypeArray.push('Residential Lots/Land');
        }
        break;
  
      case "Commercial Sale": 
        // Since this MLS puts everything under Commercial Sale, 
        // we MUST check the subtype first for Leases.
        if (subType === "Commercial Rental/Property Management") {
          propertyTypeArray.push('Commercial Lease');
        } else {
          propertyTypeArray.push('Commercial'); 
        }
        break;
  
      case "Commercial Lease":
        propertyTypeArray.push('Commercial Lease');
        break;
  
      default:
        // Safety check for the specific strings in any other type
        if (subType === "Commercial Rental/Property Management") {
          propertyTypeArray.push('Commercial Lease');
        } else if (subType === "Residential Rental/Property Management") {
          propertyTypeArray.push('Residential Lease');
        } else {
          propertyTypeArray.push(subType || "Property");
        }
    }
  
    property.ForSaleOrRent == 'R' ? 
      (saleOrRentArray.push("For Lease"), labelArray.push(["For Lease"])) : 
      (saleOrRentArray.push("For Sale"), labelArray.push(["For Sale"]))
  
    return {
      img: imgArray,
      type: "rent",
      forSaleOrForLease: property.ForSaleOrRent === 'R' ? "For Lease" : "For Sale",
      label: property.ForSaleOrRent === 'R' ? ["For Lease"] : ["For Sale"],
      country: property.City || "",
      contingentDate: property.ContingentDate || "",
      contractStatusLabel: contractStatusLabelArray,
      city: property.City || "",
      closingDate: property.CloseDate || "",
      closedPrice: property.ClosePrice || 0,
      title: `${property.StreetNumber || ""} ${property.StreetName || ""}`,
      mlsStatus: property.MlsStatus,
      modificationTimestamp: property.ModificationTimestamp || "",
      pendingTimestamp: property.Pendingtimestamp || "",
      openHouseMethod: property.OpenHouseMethod || "",
      openHouseType: property.OpenHouseType || "",
      openHouseStartTime: property.OpenHouseStartTime || "",
      openHouseEndTime: property.OpenHouseEndTime || "",
      showingAgentMlsID: property.ShowingAgentMlsID || "",
      openHouseId: property.OpenHouseId || "",
      openHouseDate: property.OpenHouseDate || "",
      openHouseKey: property.OpenHouseKe || "",
      openHouseRemarks: property.OpenHouseRemarks || "",
      price: property.ListPrice || "",
      originalListPrice: property.OriginalListPrice || "",
      priceChangeTimestamp: property.PriceChangeTimestamp || "",
      purchaseContractDate: property.PurchaseContractDate || "",
      details: property.PublicRemarks || "",
      home: "Virtual Home",
      bed: property.BedroomsTotal || "",
      bath: property.BathroomsFull || "",
      sqft: property.BuildingAreaTotal || "",
      lotSizeAcres: property.LotSizeAcres || "",
      rooms: 14,
      date: property.ListingContractDate || "",
      video: "/assets/video/video2.mp4",
      id: property.ListingId || Math.random(),
      propertyType: propertyTypeArray.length == 0 ? "Apartment" : propertyTypeArray[0],
      agencies: "Lincoln",
    }
}

function getAuctionDateFromRemarks (remarks) {
    if (!remarks) return null;
  
    // Pattern 1: Month Name + Day + Optional Year (e.g., APRIL 4th, Oct 25 2024)
    const monthPattern = /(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)[A-Z]*\s+(\d{1,2})(ST|ND|RD|TH)?/i;
    
    // Pattern 2: MM/DD/YYYY or M/D/YY (e.g., 10/25/2024)
    const slashPattern = /(\d{1,2})[\/\-]\d{1,2}[\/\-]\d{2,4}/;
  
    const monthMatch = remarks.match(monthPattern);
    const slashMatch = remarks.match(slashPattern);
  
    let dateToParse = null;
  
    if (monthMatch) {
      // Reconstruct into a parsable string, default to current year if missing
      const year = remarks.match(/\d{4}/) ? remarks.match(/\d{4}/)[0] : new Date().getFullYear();
      dateToParse = `${monthMatch[1]} ${monthMatch[2]} ${year}`;
    } else if (slashMatch) {
      dateToParse = slashMatch[0];
    }
  
    if (dateToParse) {
      const d = new Date(dateToParse);
      return isNaN(d.getTime()) ? null : d;
    }
    return null;
  };

  exports.syncMlsData = onSchedule({
    schedule: "every 60 minutes",
    timeoutSeconds: 540,
    memory: "1GiB"
    }, async (event) => {
    const token = '6e75f16c0527a8fa81baaa3795047239';
    
    // 1. SETUP DATES (Exact matches from your page.js)
    let daysToGoBack = 5;
    let daysToGoBackSold = 30;
    let daysToGoBackSoldNew = 5;
    let dateToGoBackTo = moment().subtract(daysToGoBack, "days").format("YYYY-MM-DD");
    let dateToGoBackToSold = moment().subtract(daysToGoBackSold, "days").format("YYYY-MM-DD");
    let dateToGoBackToSoldNew = moment().subtract(daysToGoBackSoldNew, "days").format("YYYY-MM-DD");
    let presentDate = moment().format("YYYY-MM-DD");
  
    const auctionExclusion = " and (PropertySubType eq null or (PropertySubType ne 'Land Auction' and PropertySubType ne 'Residential Auction'))";
    const leaseExclusion = " and (PropertySubType eq null or (PropertySubType ne 'Commercial Rental/Property Management' and PropertySubType ne 'Residential Rental/Property Management'))";
  
    // 2. GET ALL COUNTS (Your exact Promise.all block)
    const [resActive, resNew, resOH, resCont, resContNew, resSold, resSoldNew, resLandAuction, resResiAuction, resCommLease, resResiLease, resReduced, resBackOnMarket] = await Promise.all([
      fetch(`https://navapi.navicamls.net/api/v2/OData/nav17/Property?access_token=${token}&$count=true&$top=1&$filter=contains(MlsStatus,'Active')${auctionExclusion}${leaseExclusion}`).then(res => res.json()),
      fetch(`https://navapi.navicamls.net/api/v2/OData/nav17/Property?access_token=${token}&$count=true&$top=1&$filter=contains(MlsStatus,'Active') and OriginalEntryTimestamp ge ${dateToGoBackTo}${auctionExclusion}${leaseExclusion}`).then(res => res.json()),
      fetch(`https://navapi.navicamls.net/api/v2/OData/nav17/OpenHouse?access_token=${token}&$count=true&$top=1&$filter=date(OpenHouseDate) ge ${presentDate}`).then(res => res.json()),
      fetch(`https://navapi.navicamls.net/api/v2/OData/nav17/Property?access_token=${token}&$count=true&$top=1&$filter=contains(NAV17_rets_status,'Active Under Contract')${auctionExclusion}${leaseExclusion}`).then(res => res.json()),
      fetch(`https://navapi.navicamls.net/api/v2/OData/nav17/Property?access_token=${token}&$count=true&$top=1&$filter=contains(NAV17_rets_status,'Active Under Contract') and OriginalEntryTimestamp ge ${dateToGoBackTo}${auctionExclusion}${leaseExclusion}`).then(res => res.json()),
      fetch(`https://navapi.navicamls.net/api/v2/OData/nav17/Property?access_token=${token}&$count=true&$top=1&$filter=contains(MlsStatus,'Sold') and CloseDate ge ${dateToGoBackToSold}`).then(res => res.json()),
      fetch(`https://navapi.navicamls.net/api/v2/OData/nav17/Property?access_token=${token}&$count=true&$top=1&$filter=contains(MlsStatus,'Sold') and OriginalEntryTimestamp ge ${dateToGoBackToSoldNew}${auctionExclusion}${leaseExclusion}`).then(res => res.json()),
      fetch(`https://navapi.navicamls.net/api/v2/OData/nav17/Property?access_token=${token}&$count=true&$top=1&$filter=contains(PropertySubType,'Land Auction') and CloseDate eq null`).then(res => res.json()),
      fetch(`https://navapi.navicamls.net/api/v2/OData/nav17/Property?access_token=${token}&$count=true&$top=1&$filter=contains(PropertySubType,'Residential Auction') and CloseDate eq null`).then(res => res.json()),
      fetch(`https://navapi.navicamls.net/api/v2/OData/nav17/Property?access_token=${token}&$count=true&$top=1&$filter=contains(PropertySubType,'Commercial Rental/Property Management') and CloseDate eq null`).then(res => res.json()),
      fetch(`https://navapi.navicamls.net/api/v2/OData/nav17/Property?access_token=${token}&$count=true&$top=1&$filter=contains(PropertySubType,'Residential Rental/Property Management') and CloseDate eq null`).then(res => res.json()),
      fetch(`https://navapi.navicamls.net/api/v2/OData/nav17/Property?access_token=${token}&$count=true&$top=1&$filter=contains(MlsStatus,'Active') and ListPrice lt OriginalListPrice${auctionExclusion}`).then(res => res.json()),
      fetch(`https://navapi.navicamls.net/api/v2/OData/nav17/Property?access_token=${token}&$count=true&$top=1&$filter=MlsStatus eq 'Active' and ContingentDate ne null${auctionExclusion}${leaseExclusion}`).then(res => res.json()),
    ]);
  
    // 3. CONSTRUCT URLS FOR EVERY CATEGORY (Looping by 200)
    let propUrls = [];
    let ohUrls = [];

    let apiSort = "OriginalEntryTimestamp desc";
  
    const addUrls = (count, baseUrl) => {
      for (let i = 0; i < Math.ceil(count / 200); i++) {
        propUrls.push(`${baseUrl}&$skip=${200 * i}&$top=200&$orderby=${apiSort}`);
      }
    };


  
    // Add all category fetches to the queue
    addUrls(resActive['@odata.count'] + resCont['@odata.count'], `https://navapi.navicamls.net/api/v2/OData/nav17/Property?access_token=${token}&$filter=(contains(MlsStatus,'Active') or contains(NAV17_rets_status,'Active Under Contract'))${auctionExclusion}${leaseExclusion}`);
    addUrls(resSold['@odata.count'], `https://navapi.navicamls.net/api/v2/OData/nav17/Property?access_token=${token}&$filter=contains(MlsStatus,'Sold') and CloseDate ge ${dateToGoBackToSold}`);
    addUrls(resReduced['@odata.count'], `https://navapi.navicamls.net/api/v2/OData/nav17/Property?access_token=${token}&$filter=contains(MlsStatus,'Active') and ListPrice lt OriginalListPrice${auctionExclusion}`);
    
    // Auctions
    addUrls(resLandAuction['@odata.count'], `https://navapi.navicamls.net/api/v2/OData/nav17/Property?access_token=${token}&$filter=contains(PropertySubType,'Land Auction') and CloseDate eq null`);
    addUrls(resResiAuction['@odata.count'], `https://navapi.navicamls.net/api/v2/OData/nav17/Property?access_token=${token}&$filter=contains(PropertySubType,'Residential Auction') and CloseDate eq null`);

    // Leases
    addUrls(resCommLease['@odata.count'], `https://navapi.navicamls.net/api/v2/OData/nav17/Property?access_token=${token}&$filter=contains(PropertySubType,'Commercial Rental/Property Management') and CloseDate eq null`);
    addUrls(resResiLease['@odata.count'], `https://navapi.navicamls.net/api/v2/OData/nav17/Property?access_token=${token}&$filter=contains(PropertySubType,'Residential Rental/Property Management') and CloseDate eq null`);

    // Add Open House URLs separately
    for (let i = 0; i < Math.ceil(resOH['@odata.count'] / 200); i++) {
      ohUrls.push(`https://navapi.navicamls.net/api/v2/OData/nav17/OpenHouse?access_token=${token}&$skip=${200 * i}&$top=200&$orderby=OpenHouseDate desc&$filter=date(OpenHouseDate) ge ${presentDate}`);
    }
  
    // 4. FETCH, CLEAN, AND MERGE
    const fetchAndClean = async (url) => {
      const res = await fetch(url);
      const text = await res.text();
      const cleaned = text.replace(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]/g, ""); 
      try { return JSON.parse(cleaned); } catch (e) { return { value: [] }; }
    };
  
    const [propResponses, ohResponses] = await Promise.all([
      Promise.all(propUrls.map(fetchAndClean)),
      Promise.all(ohUrls.map(fetchAndClean))
    ]);
  
    const mapProps = new Map();
    propResponses.flatMap(r => r.value).forEach(item => mapProps.set(item.ListingId, item));
    ohResponses.flatMap(r => r.value).forEach(item => {
      if (mapProps.has(item.ListingId)) {
        mapProps.set(item.ListingId, { ...mapProps.get(item.ListingId), ...item });
      }
    });
  
    // 5. FINAL BATCH SAVE TO FIRESTORE


let batch = db.batch();
let count = 0;

for (const p of mapProps.values()) {
  const processed = computePropertyLabels(p);
  
  const mlsStatus = String(p.MlsStatus || "").toLowerCase();
  const retsStatus = String(p.NAV17_rets_status || "").toLowerCase();
  const subType = String(p.PropertySubType || "").trim();
  const original = Number(p.OriginalListPrice || 0);
  const current = Number(p.ListPrice || 0);
  const redPct = original > 0 ? (original - current) / original : 0;
  
  const isAuction = subType === "Land Auction" || subType === "Residential Auction";
  const isLease = subType === "Commercial Rental/Property Management" || subType === "Residential Rental/Property Management";
  const isContingent = retsStatus.includes('active under contract');

  processed.catFlags = {
    AllListings: (mlsStatus === 'active' || isContingent) && !isAuction && !isLease,
    Condos: subType === 'Condominium' && (mlsStatus === 'active' || isContingent),
    Land: (processed.propertyType === 'Residential Lots/Land' || processed.propertyType === 'Land Auction') && 
          (mlsStatus === 'active' || isContingent) && !isAuction,
    ContingentListings: isContingent && !isAuction && !isLease,
    ReducedListings: (original > current && redPct < 0.90) && !isAuction && !isLease,
    NewListings: processed.contractStatusLabel.includes('just listed') && !isAuction && !isLease,
    OpenHouseListings: processed.contractStatusLabel.includes('open house') && !isAuction && !isLease,
    BackOnMarketListings: (mlsStatus === 'active' && p.ContingentDate != null) && !isAuction && !isLease,
    SoldListings: mlsStatus === 'sold',
    LandAuction: subType === 'Land Auction',
    ResidentialAuction: subType === 'Residential Auction',
    CommercialLease: subType === 'Commercial Rental/Property Management',
    ResidentialLease: subType === 'Residential Rental/Property Management'
  };

  // --- THE CRITICAL SORT FIX --- 
  // We save the raw Navica ISO string (2025-03-28T...) into these fields.
  // DO NOT use M-D-YYYY here or sorting will fail forever.
  processed.price = current; 
  processed.originalListPrice = original;
  processed.reductionPercentage = redPct;
  processed.priceReductionAmount = original > current ? (original - current) : 0;
  processed.originalEntryTimestamp = p.OriginalEntryTimestamp || "";
  processed.closeDate = p.CloseDate || "";
  processed.priceChangeTimestamp = p.PriceChangeTimestamp || "";
  processed.modificationTimestamp = p.ModificationTimestamp || "";
  // Set the generic date field to raw ISO as well for a fallback
  processed.date = p.OriginalEntryTimestamp || p.ListingContractDate || "";

  const docRef = db.collection('listings').doc(String(p.ListingId));
  batch.set(docRef, processed, { merge: true });
  
  count++;
  if (count % 500 === 0) {
    await batch.commit();
    batch = db.batch();
  }
}
await batch.commit();
    return null;
});

// To deploy this function use in this directory (functions)
// firebase deploy --only functions:sendAutomatedEmailAlerts
exports.sendAutomatedEmailAlerts = onSchedule({
    schedule: "every 60 minutes",
    timeoutSeconds: 540,
    memory: "1GiB",
    secrets: ["GMAIL_PRIVATE_KEY"]
}, async (event) => {
    try {
        const privateKey = process.env.GMAIL_PRIVATE_KEY.replace(/\\n/g, '\n');
        // // 1. SETUP GMAIL AUTH
        // const auth = new google.auth.JWT(
        //     'listing-alerts-sender@ben-carlen.iam.gserviceaccount.com',
        //     null,
        //     privateKey,
        //     ['https://www.googleapis.com/auth/gmail.send'], // Use the full .send scope
        //     'Ben@BenCarlen.com'
        console.log('Key length:', process.env.GMAIL_PRIVATE_KEY?.length)
        const auth = new google.auth.JWT({
            email: 'listing-alerts-sender@ben-carlen.iam.gserviceaccount.com',
            key: privateKey, // Your processed key
            scopes: ['https://www.googleapis.com/auth/gmail.send'],
            subject: 'Ben@BenCarlen.com'
        })
       
        await auth.authorize()
        const gmail = google.gmail({ version: 'v1', auth });

        // 2. DEFINE THE TIME WINDOW (Last 60 Minutes)
        const inLastHour = new Date(Date.now() - 60 * 60 * 1000).toISOString();
        const inLastDay = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString()

        // 3. QUERY FIRESTORE FOR MATCHES
        // Note: 'originalEntryTimestamp' is what you used in your scraper save logic
        const listingsSnap = await db.collection('listings')
            .where('country', '==', 'Cookeville')
            .where('price', '>=', 500000)
            .where('originalEntryTimestamp', '>=', inLastHour)
            // .limit(2)
            .get();

        if (listingsSnap.empty) {
            console.log("No new Cookeville listings over $500k in the last hour.");
            return null;
        }

        // 4. BUILD THE EMAIL CONTENT
        let listingsHtml = "";
        listingsSnap.forEach(doc => {
            const data = doc.data();
            const listingId = data.mlsNumber || doc.id;
            const details = [
                data.bed ? `Beds: ${data.bed}` : null,
                data.bath ? `Baths: ${data.bath}` : null
              ].filter(Boolean).join(' | ');
              
              const bedAndBathHtml = `<p>${details}</p>`;
            listingsHtml += `
                <div style="border-bottom: 1px solid #ddd; padding: 10px 0;">
                    <h3 style="margin:0;">${data.UnparsedAddress || 'New Listing'}</h3>
                    <p>Price: <b>$${data.price?.toLocaleString()}</b></p>
                    ${bedAndBathHtml}
                    <a href="https://bencarlen.com/without-top?id=${listingId}">View Details</a>
                </div>`;
        });

        // 5. CONSTRUCT AND SEND THE EMAIL
        const rawMessage = [
            'From: "Ben Carlen" <Ben@BenCarlen.com>',
            'To: Ben@BenCarlen.com', 
            // lccarlen@gmail.com', 
            'Subject: New Cookeville Listings Found!',
            'MIME-Version: 1.0',
            'Content-Type: text/html; charset=utf-8',
            '',
            `<h2>Test Alert: Cookeville $500k+</h2>
             <p>The following listings that meet your criteria hit the market in the last hour:</p>
             ${listingsHtml}
             <p><small>This is an automated test alert.</small></p>`
        ].join('\n');

        const encodedMessage = Buffer.from(rawMessage)
            .toString('base64')
            .replace(/\+/g, '-')
            .replace(/\//g, '_')
            .replace(/=+$/, '');

        await gmail.users.messages.send({
            userId: 'me',
            requestBody: { raw: encodedMessage }
        });

        console.log(`Successfully sent alert for ${listingsSnap.size} listings.`);
    } catch (error) {
        console.error("Failed to send alerts:", error);
    }
});