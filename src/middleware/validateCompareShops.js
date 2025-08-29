const { body, validationResult } = require("express-validator");

// Validation rules for compare shops endpoint
const validateCompareShops = [
  body("lat")
    .isFloat({ min: -90, max: 90 })
    .withMessage("Latitude must be a number between -90 and 90"),

  body("lng")
    .isFloat({ min: -180, max: 180 })
    .withMessage("Longitude must be a number between -180 and 180"),

  body("radius")
    .optional()
    .isInt({ min: 100, max: 50000 })
    .withMessage("Radius must be an integer between 100 and 50000 meters"),

  // Check if Google Maps API key is configured
  (req, res, next) => {
    if (!process.env.GOOGLE_API_KEY) {
      return res.status(500).json({
        success: false,
        error: "Google Maps API key not configured",
      });
    }
    next();
  },
];

// Middleware to handle validation errors
const handleValidationErrors = (req, res, next) => {
  const errors = validationResult(req);
  if (!errors.isEmpty()) {
    return res.status(400).json({
      success: false,
      error: "Validation failed",
      details: errors.array(),
    });
  }
  next();
};

module.exports = {
  validateCompareShops,
  handleValidationErrors,
};
