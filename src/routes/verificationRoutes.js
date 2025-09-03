const express = require("express");
const router = express.Router();
const verificationController = require("../controllers/verificationController");
const {
  authMiddleware,
  adminMiddleware,
} = require("../middleware/authMiddleware");

// User routes (require authentication)
router.post(
  "/claim",
  authMiddleware,
  verificationController.createClaimRequest,
);

// Admin routes
router.get(
  "/admin/pending",
  adminMiddleware,
  verificationController.getAdminPendingRequests,
);
router.post(
  "/:requestId/approve",
  adminMiddleware,
  verificationController.approveRequest,
);
router.post(
  "/:requestId/reject",
  adminMiddleware,
  verificationController.rejectRequest,
);

module.exports = router;
