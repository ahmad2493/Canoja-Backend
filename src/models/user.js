const mongoose = require("mongoose");

const userSchema = new mongoose.Schema(
  {
    email: {
      type: String,
      required: true,
      lowercase: true,
      trim: true,
    },
    name: { type: String, default: "" },
    password: { type: String, required: true },
    role: {
      type: String,
      enum: ["consumer", "operator", "admin"],
      default: "consumer",
    },
    plan_tier: {
      type: String,
      enum: ["free", "starter", "pro"],
      default: "free",
      index: true,
    },
    licenseRecords: [
      {
        type: mongoose.Schema.Types.ObjectId,
        ref: "LicenseRecord",
      },
    ],
    requiresPasswordChange: {
      type: Boolean,
      default: false,
    },
    refreshToken: {
      type: String,
      default: null,
      index: true,
    },
    refreshTokenExpiresAt: {
      type: Date,
      default: null,
    },
    isActive: {
      type: Boolean,
      default: true,
      index: true,
    },
  },
  { timestamps: true },
);

module.exports = mongoose.model("User", userSchema);
