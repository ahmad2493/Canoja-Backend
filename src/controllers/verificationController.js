const VerificationRequest = require("../models/verificationRequest");
const LicenseRecord = require("../models/licenseRecord");
const User = require("../models/user");
const upload = require("../utils/s3Upload");

const uploadFields = upload.fields([
  { name: "state_license_document", maxCount: 1 },
  { name: "utility_bill", maxCount: 1 },
  { name: "government_issued_id_document", maxCount: 1 },
]);

const createClaimRequest = async (req, res) => {
  try {
    const {
      pharmacyId,
      legal_business_name,
      physical_address,
      business_phone_number,
      website_or_social_media_link,
      contact_person,
      license_information,
      gps_coordinates,
    } = req.body;

    const userId = req.user.id;

    // Parse contact_person and license_information if they're strings (from form data)
    let parsedContactPerson, parsedLicenseInfo;
    try {
      parsedContactPerson =
        typeof contact_person === "string"
          ? JSON.parse(contact_person)
          : contact_person;
      parsedLicenseInfo =
        typeof license_information === "string"
          ? JSON.parse(license_information)
          : license_information;
    } catch (parseError) {
      return res.status(400).json({
        success: false,
        error: "Invalid JSON format for contact_person or license_information",
      });
    }

    // Validate required fields
    if (
      !pharmacyId ||
      !legal_business_name ||
      !physical_address ||
      !business_phone_number ||
      !parsedContactPerson ||
      !parsedLicenseInfo
    ) {
      return res.status(400).json({
        success: false,
        error: "Missing required fields",
      });
    }

    // Determine if pharmacyId is a LicenseRecord ObjectId or Google place_id
    let isLicenseRecord = false;
    let licenseRecord = null;
    let claimRequested = true; // Always true since user is claiming
    let verifyRequested = true; // Default to true

    // Check if pharmacyId is a valid ObjectId (24 hex characters) - LicenseRecord
    if (/^[0-9a-fA-F]{24}$/.test(pharmacyId)) {
      console.log(`Checking LicenseRecord for ID: ${pharmacyId}`);
      licenseRecord = await LicenseRecord.findById(pharmacyId);

      if (licenseRecord) {
        isLicenseRecord = true;
        console.log(
          `Found LicenseRecord: ${licenseRecord.business_name}, canojaVerified: ${licenseRecord.canojaVerified}`,
        );

        // If it's already verified, only claim is needed
        if (licenseRecord.canojaVerified) {
          verifyRequested = false;
          claimRequested = true;
          console.log("LicenseRecord is already verified, only claiming");
        } else {
          verifyRequested = true;
          claimRequested = true;
          console.log(
            "LicenseRecord not verified, requesting both verify and claim",
          );
        }
      } else {
        // Invalid ObjectId but not found in database
        return res.status(404).json({
          success: false,
          error: "License record not found with provided ID",
        });
      }
    } else {
      // It's a Google place_id - not verified and not claimed
      isLicenseRecord = false;
      verifyRequested = true;
      claimRequested = true;
      console.log(
        `Google place_id detected: ${pharmacyId}, requesting both verify and claim`,
      );
    }

    // Check for existing pending claim for this pharmacyId
    const existingClaim = await VerificationRequest.findOne({
      pharmacyId: pharmacyId,
      claimRequested: true,
      status: "pending",
    });

    if (existingClaim) {
      return res.status(400).json({
        success: false,
        error: "A claim request is already pending for this business",
      });
    }

    // Prepare uploaded documents URLs (S3 handling remains unchanged)
    const uploadedDocuments = {};
    if (req.files) {
      if (req.files.state_license_document) {
        uploadedDocuments.state_license_document =
          req.files.state_license_document[0].location;
      }
      if (req.files.utility_bill) {
        uploadedDocuments.utility_bill = req.files.utility_bill[0].location;
      }
    }

    // Handle government ID document
    let govIdDocument = null;
    if (req.files && req.files.government_issued_id_document) {
      govIdDocument = req.files.government_issued_id_document[0].location;
    }

    console.log(
      `Creating verification request with: claimRequested=${claimRequested}, verifyRequested=${verifyRequested}`,
    );

    // Create verification request with S3 URLs
    const verificationRequest = new VerificationRequest({
      pharmacyId,
      claimRequested: claimRequested,
      verifyRequested: verifyRequested,
      userId,
      adminVerifiedRequired: true,
      notes: `Claim request by user ${userId}. ${isLicenseRecord ? "LicenseRecord" : "Google place"}: ${pharmacyId}`,

      // Basic Business Information
      legal_business_name,
      physical_address,
      business_phone_number,
      website_or_social_media_link,

      // Contact Person Information
      contact_person: {
        full_name: parsedContactPerson.full_name,
        email_address: parsedContactPerson.email_address,
        phone_number: parsedContactPerson.phone_number,
        role_or_position: parsedContactPerson.role_or_position,
        government_issued_id_document: govIdDocument,
      },

      // License Information
      license_information: {
        license_number: parsedLicenseInfo.license_number,
        issuing_authority: parsedLicenseInfo.issuing_authority,
        license_type: parsedLicenseInfo.license_type,
        expiration_date: parsedLicenseInfo.expiration_date,
        jurisdiction: parsedLicenseInfo.jurisdiction,
      },

      // Document uploads (S3 URLs - unchanged)
      uploaded_documents: uploadedDocuments,

      // GPS coordinates
      gps_coordinates: gps_coordinates ? JSON.parse(gps_coordinates) : {},

      // Submission metadata
      verification_metadata: {
        ip_address: req.ip,
        user_agent: req.get("User-Agent"),
      },
    });

    await verificationRequest.save();

    // Build response message based on what was requested
    let message =
      "Claim request submitted successfully. Admin review required.";
    if (verifyRequested && claimRequested) {
      message =
        "Claim and verification requests submitted successfully. Admin review required.";
    } else if (claimRequested && !verifyRequested) {
      message = "Claim request submitted successfully. Admin review required.";
    }

    res.json({
      success: true,
      message,
      data: {
        requestId: verificationRequest._id,
        pharmacyId: pharmacyId,
        claimRequested: claimRequested,
        verifyRequested: verifyRequested,
        isLicenseRecord: isLicenseRecord,
        status: "pending",
        uploadedFiles: {
          state_license_document:
            uploadedDocuments.state_license_document || null,
          utility_bill: uploadedDocuments.utility_bill || null,
          government_issued_id_document: govIdDocument || null,
        },
      },
    });
  } catch (error) {
    console.error("Create claim request error:", error);
    res.status(500).json({
      success: false,
      error: "Failed to create claim request",
      details: error.message,
    });
  }
};

const getAdminPendingRequests = async (req, res) => {
  try {
    const { requestType } = req.query;

    let filter = { status: "pending" };

    if (requestType === "claim") {
      filter.claimRequested = true;
    } else if (requestType === "verify") {
      filter.verifyRequested = true;
    } else if (requestType === "both") {
      filter.claimRequested = true;
      filter.verifyRequested = true;
    }

    const requests = await VerificationRequest.find(filter).sort({
      createdAt: -1,
    });

    res.json({
      success: true,
      requests,
    });
  } catch (error) {
    console.error("Get pending requests error:", error);
    res.status(500).json({
      success: false,
      error: "Failed to fetch pending requests",
    });
  }
};

const approveRequest = async (req, res) => {
  try {
    const { requestId } = req.params;

    // Find the request and populate user data
    const request = await VerificationRequest.findById(requestId);

    if (!request) {
      return res.status(404).json({
        success: false,
        error: "Request not found",
      });
    }

    // Update request status
    request.status = "approved";
    request.adminVerifiedRequired = false;

    let licenseRecord = null;
    let isExistingLicenseRecord = false;
    let licenseRecordId = null;

    // Check if pharmacyId is an existing LicenseRecord ObjectId
    if (/^[0-9a-fA-F]{24}$/.test(request.pharmacyId)) {
      licenseRecord = await LicenseRecord.findById(request.pharmacyId);
      if (licenseRecord) {
        isExistingLicenseRecord = true;
        licenseRecordId = request.pharmacyId;
        console.log(
          `Found existing LicenseRecord: ${licenseRecord.business_name}`,
        );
      }
    }

    // Handle existing LicenseRecord
    if (isExistingLicenseRecord) {
      console.log(
        `Processing existing LicenseRecord - verifyRequested: ${request.verifyRequested}, claimRequested: ${request.claimRequested}`,
      );

      // Update the license record based on what was requested
      const updateFields = {};

      if (request.claimRequested) {
        updateFields.claimed = true;
        updateFields.claimedBy = request.userId;
        updateFields.claimedAt = new Date();
        console.log("Marking license as claimed");
      }

      if (request.verifyRequested) {
        updateFields.canojaVerified = true;
        updateFields.adminVerificationRequired = false;
        console.log("Marking license as verified");
      }

      await LicenseRecord.findByIdAndUpdate(licenseRecordId, updateFields);

      // Update user role to operator and add license to their array (only if claiming)
      if (request.claimRequested) {
        try {
          await User.findByIdAndUpdate(request.userId, {
            role: "operator",
            $addToSet: { licenseRecords: licenseRecordId },
          });
          console.log(
            `Updated user ${request.userId} role to operator and added license ${licenseRecordId}`,
          );
        } catch (updateError) {
          console.error("Failed to update user:", updateError);
          await request.save();
          return res.json({
            success: true,
            message: "Request approved, but failed to update user role",
            request,
            userRoleUpdated: false,
            error: updateError.message,
          });
        }
      }
    } else {
      // Handle Google place_id - create new LicenseRecord
      console.log(
        `Creating new LicenseRecord for Google place: ${request.pharmacyId}`,
      );

      licenseRecord = new LicenseRecord({
        business_name: request.legal_business_name,
        license_number: request.license_information.license_number,
        stateName: request.license_information.jurisdiction,
        city: extractCityFromAddress(request.physical_address),
        business_address: request.physical_address,
        contact_information: {
          phone: request.business_phone_number,
          email: request.contact_person.email_address,
          website: request.website_or_social_media_link,
        },
        owner: {
          name: request.contact_person.full_name,
          email: request.contact_person.email_address,
          role: request.contact_person.role_or_position,
          phone: request.contact_person.phone_number,
          govt_issued_id: request.contact_person.government_issued_id_document,
        },
        expiration_date: request.license_information.expiration_date,
        license_type: request.license_information.license_type,
        jurisdiction: request.license_information.jurisdiction,
        regulatory_body: request.license_information.issuing_authority,
        state_license_document:
          request.uploaded_documents?.state_license_document,
        utility_bill: request.uploaded_documents?.utility_bill,
        gps_validation: request.gps_validation_status === "validated",
        location: {
          type: "Point",
          coordinates:
            request.gps_coordinates?.longitude &&
            request.gps_coordinates?.latitude
              ? [
                  request.gps_coordinates.longitude,
                  request.gps_coordinates.latitude,
                ]
              : undefined,
        },
        claimed: true,
        claimedBy: request.userId,
        claimedAt: new Date(),
        canojaVerified: true,
        adminVerificationRequired: false,
        googlePlaceId: request.pharmacyId,
      });

      await licenseRecord.save();
      licenseRecordId = licenseRecord._id;

      console.log(`Created new LicenseRecord with ID: ${licenseRecordId}`);

      // Update the request to reference the new LicenseRecord
      request.pharmacyId = licenseRecordId.toString();

      // Update user role to operator and add license to their array
      try {
        await User.findByIdAndUpdate(request.userId, {
          role: "operator",
          $addToSet: { licenseRecords: licenseRecordId },
        });
        console.log(
          `Updated user ${request.userId} role to operator and added new license ${licenseRecordId}`,
        );
      } catch (updateError) {
        console.error("Failed to update user:", updateError);
        await request.save();
        return res.json({
          success: true,
          message:
            "Request approved and license record created, but failed to update user role",
          request,
          licenseRecordCreated: true,
          userRoleUpdated: false,
          error: updateError.message,
        });
      }
    }

    // Mark the specific requests as processed
    if (request.claimRequested) {
      request.claimRequested = false; // Mark as processed
    }
    if (request.verifyRequested) {
      request.verifyRequested = false; // Mark as processed
    }

    await request.save();

    // Build response message
    let message = "Request approved successfully";
    let actionsTaken = [];

    if (request.claimRequested) {
      actionsTaken.push("business claimed");
    }
    if (request.verifyRequested) {
      actionsTaken.push("business verified");
    }
    if (!isExistingLicenseRecord) {
      actionsTaken.push("new license record created");
    }

    if (actionsTaken.length > 0) {
      message = `Request approved successfully - ${actionsTaken.join(", ")}`;
    }

    res.json({
      success: true,
      message,
      request,
      data: {
        licenseRecordCreated: !isExistingLicenseRecord,
        licenseRecordId: licenseRecordId,
        userRoleUpdated: true,
        claimProcessed: request.claimRequested,
        verificationProcessed: request.verifyRequested,
        actions: actionsTaken,
      },
    });
  } catch (error) {
    console.error("Approve request error:", error);
    res.status(500).json({
      success: false,
      error: "Failed to approve request",
      details: error.message,
    });
  }
};

const rejectRequest = async (req, res) => {
  try {
    const { requestId } = req.params;
    const { reason } = req.body;

    const request = await VerificationRequest.findById(requestId);

    if (!request) {
      return res.status(404).json({
        success: false,
        error: "Request not found",
      });
    }

    request.status = "rejected";
    if (reason) {
      request.notes = (request.notes || "") + `\nRejection reason: ${reason}`;
    }
    await request.save();

    res.json({
      success: true,
      message: "Request rejected successfully",
      request,
    });
  } catch (error) {
    console.error("Reject request error:", error);
    res.status(500).json({
      success: false,
      error: "Failed to reject request",
    });
  }
};

function extractCityFromAddress(address) {
  if (!address) return null;

  const parts = address.split(",");
  if (parts.length >= 2) {
    return parts[parts.length - 2].trim();
  }
  return null;
}

module.exports = {
  createClaimRequest,
  getAdminPendingRequests,
  approveRequest,
  rejectRequest,
  uploadFields,
};
