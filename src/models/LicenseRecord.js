const mongoose = require("mongoose");

const licenseRecordSchema = new mongoose.Schema(
  {
    googlePlaceId: {
      type: String,
      default: null,
    },
    business_name: String,
    license_number: String,
    stateName: String,
    city: String,
    business_address: String,
    contact_information: {
      phone: String,
      email: String,
      website: String,
    },
    owner: {
      name: String,
      email: String,
      role: String,
      phone: String,
      govt_issued_id: String,
    },
    operator_name: String,
    issue_date: Date,
    expiration_date: Date,
    license_type: String,
    license_status: String,
    jurisdiction: String,
    regulatory_body: String,
    entity_type: [String],
    filing_documents_url: String,
    license_conditions: [String],
    claimed: { type: Boolean, default: false },
    claimedBy: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "User",
      default: null,
    },
    claimedAt: {
      type: Date,
      default: null,
    },
    canojaVerified: { type: Boolean, default: false },
    adminVerificationRequired: { type: Boolean, default: false },
    featured: { type: Boolean, default: false },
    // From verification process
    dba: String,
    // Documents
    state_license_document: String,
    utility_bill: String,
    gps_validation: Boolean,
    location: {
      type: {
        type: String,
        default: "Point",
      },
      coordinates: {
        type: [Number], // [longitude, latitude]
      },
    },
    smoke_shop: { type: Boolean, default: false },
  },
  {
    timestamps: true, // This adds createdAt and updatedAt fields
  },
);

// Create 2dsphere index for geospatial queries
licenseRecordSchema.index({ location: "2dsphere" });

// Add other useful indexes
licenseRecordSchema.index({ business_name: 1 });
licenseRecordSchema.index({ license_number: 1 });
licenseRecordSchema.index({ license_status: 1 });
licenseRecordSchema.index({ city: 1, stateName: 1 });
licenseRecordSchema.index({ smoke_shop: 1 });

module.exports = mongoose.model("LicenseRecord", licenseRecordSchema);
