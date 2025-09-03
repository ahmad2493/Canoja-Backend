// let mongoose = require("mongoose");
// let User = mongoose.model("User");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");

async function login(req, res) {
  try {
    const { email, password } = req.body;

    // Check against environment variables
    const adminEmail = process.env.ADMIN_EMAIL;
    const adminPasswordHash = process.env.ADMIN_PASSWORD_HASH;

    if (!adminEmail || !adminPasswordHash) {
      return res
        .status(500)
        .json({ error: "Admin credentials not configured" });
    }

    // Validate email
    if (email !== adminEmail) {
      return res.status(401).json({ error: "Invalid credentials" });
    }

    // Validate password against hash
    const isValidPassword = await bcrypt.compare(password, adminPasswordHash);
    if (!isValidPassword) {
      return res.status(401).json({ error: "Invalid credentials" });
    }

    // Generate JWT token
    const token = jwt.sign(
      {
        email: adminEmail,
        role: "admin",
        isEnvAdmin: true,
      },
      process.env.JWT_SECRET || "your-secret-key",
      { expiresIn: "24h" },
    );

    res.json({
      token,
      user: {
        email: adminEmail,
        role: "admin",
      },
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
}

async function listUsers(req, res) {
  try {
    const users = await User.find({}, "-password"); // Exclude password
    res.json(users);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
}

async function createUser(req, res) {
  try {
    const { email, password, role } = req.body;
    const hashedPassword = await bcrypt.hash(password, 10);
    const user = new User({ email, password: hashedPassword, role });
    await user.save();
    res
      .status(201)
      .json({
        message: "User created",
        user: { email: user.email, role: user.role, _id: user._id },
      });
  } catch (error) {
    res.status(400).json({ error: error.message });
  }
}

async function changeUserRole(req, res) {
  try {
    const { userId } = req.params;
    const { role } = req.body;
    const user = await User.findByIdAndUpdate(
      userId,
      { role },
      { new: true, runValidators: true },
    );
    if (!user) return res.status(404).json({ error: "User not found" });
    res.json({
      message: "Role updated",
      user: { email: user.email, role: user.role, _id: user._id },
    });
  } catch (error) {
    res.status(400).json({ error: error.message });
  }
}

module.exports = {
  login,
  listUsers,
  createUser,
  changeUserRole,
};
