const jwt = require("jsonwebtoken");
const User = require("../models/User");

// Regular user authentication middleware
const authMiddleware = async (req, res, next) => {
  try {
    const token = req.header("Authorization")?.replace("Bearer ", "");

    if (!token) {
      return res.status(401).json({
        success: false,
        error: "Access denied. No token provided.",
      });
    }

    const decoded = jwt.verify(token, process.env.JWT_SECRET);
    const user = await User.findById(decoded.userId).select("-password");

    if (!user) {
      return res.status(401).json({
        success: false,
        error: "Invalid token. User not found.",
      });
    }

    req.user = user;
    next();
  } catch (error) {
    console.error("Auth middleware error:", error);
    res.status(401).json({
      success: false,
      error: "Invalid token.",
    });
  }
};

// Admin authentication middleware
const adminMiddleware = async (req, res, next) => {
  try {
    const token = req.header("Authorization")?.replace("Bearer ", "");

    if (!token) {
      return res.status(401).json({
        success: false,
        error: "Access denied. No token provided.",
      });
    }

    const decoded = jwt.verify(token, process.env.JWT_SECRET);
    const admin = await User.findById(decoded.userId).select("-password");

    if (!admin) {
      return res.status(401).json({
        success: false,
        error: "Invalid token. Admin not found.",
      });
    }

    req.admin = admin;
    next();
  } catch (error) {
    console.error("Admin middleware error:", error);
    res.status(401).json({
      success: false,
      error: "Invalid admin token.",
    });
  }
};

module.exports = {
  authMiddleware,
  adminMiddleware,
};
