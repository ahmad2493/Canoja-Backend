const mongoose = require("mongoose");

const verificationRequestSchema = new mongoose.Schema(
  {
    pharmacy: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "LicenseRecord",
      required: true,
    },
    status: {
      type: String,
      enum: ["pending", "approved", "rejected"],
      default: "pending",
    },
    adminVerifiedRequired: {
      type: Boolean,
      default: false,
    },
    requestType: {
      type: String,
      enum: ["claim", "verify"],
      default: "claim",
    },
    notes: {
      type: String,
    },
  },
  { timestamps: true },
);

module.exports = mongoose.model(
  "VerificationRequest",
  verificationRequestSchema,
);
