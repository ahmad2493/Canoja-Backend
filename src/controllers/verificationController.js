const VerificationRequest = require("../models/VerificationRequest");
const LicenseRecord = require("../models/licenseRecord");

// =============================================================================
// BUSINESS CLAIM CONTROLLERS
// =============================================================================

/**
 * Create claim request (when user clicks "Is this your business?")
 */
const createClaimRequest = async (req, res) => {
  try {
    const { licenseRecordId } = req.body;
    const userId = req.user.id;

    // Find the license record
    const licenseRecord = await LicenseRecord.findById(licenseRecordId);
    if (!licenseRecord) {
      return res.status(404).json({
        success: false,
        error: "License record not found",
      });
    }

    // Check if user already has a pending claim for this license
    const existingClaim = await VerificationRequest.findOne({
      pharmacy: licenseRecordId,
      requestType: "claim",
      status: "pending",
    });

    if (existingClaim) {
      return res.status(400).json({
        success: false,
        error: "A claim request is already pending for this business",
      });
    }

    // Determine if verification is also needed
    const needsVerification = !licenseRecord.canoja_verified;

    // Create verification request first if needed
    if (needsVerification) {
      const verificationRequest = new VerificationRequest({
        pharmacy: licenseRecordId,
        requestType: "verify",
        adminVerifiedRequired: true,
        notes: `Verification requested by user ${userId} as part of claim process`,
      });
      await verificationRequest.save();
    }

    // Create claim request
    const claimRequest = new VerificationRequest({
      pharmacy: licenseRecordId,
      requestType: "claim",
      adminVerifiedRequired: true,
      notes: `Claim requested by user ${userId}`,
    });

    await claimRequest.save();

    res.json({
      success: true,
      message: needsVerification
        ? "Claim and verification requests submitted. Admin review required."
        : "Claim request submitted. Admin review required.",
      needsVerification,
      claimRequestId: claimRequest._id,
    });
  } catch (error) {
    console.error("Create claim request error:", error);
    res.status(500).json({
      success: false,
      error: "Failed to create claim request",
    });
  }
};

// =============================================================================
// ADMIN CONTROLLERS
// =============================================================================

/**
 * Get pending verification and claim requests for admin
 */
const getAdminPendingRequests = async (req, res) => {
  try {
    const { requestType } = req.query;

    let filter = { status: "pending" };
    if (requestType) {
      filter.requestType = requestType;
    }

    const requests = await VerificationRequest.find(filter)
      .populate("pharmacy")
      .sort({ createdAt: -1 });

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

/**
 * Admin approve request
 */
const approveRequest = async (req, res) => {
  try {
    const { requestId } = req.params;

    const request =
      await VerificationRequest.findById(requestId).populate("pharmacy");

    if (!request) {
      return res.status(404).json({
        success: false,
        error: "Request not found",
      });
    }

    // Update request status
    request.status = "approved";
    await request.save();

    // If it's a verification request, update the license record
    if (request.requestType === "verify") {
      await LicenseRecord.findByIdAndUpdate(request.pharmacy._id, {
        canoja_verified: true,
      });
    }

    res.json({
      success: true,
      message: `${request.requestType} request approved successfully`,
      request,
    });
  } catch (error) {
    console.error("Approve request error:", error);
    res.status(500).json({
      success: false,
      error: "Failed to approve request",
    });
  }
};

/**
 * Admin reject request
 */
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

    // Update request status
    request.status = "rejected";
    if (reason) {
      request.notes = (request.notes || "") + `\nRejection reason: ${reason}`;
    }
    await request.save();

    res.json({
      success: true,
      message: `${request.requestType} request rejected`,
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

module.exports = {
  createClaimRequest,
  getAdminPendingRequests,
  approveRequest,
  rejectRequest,
};
