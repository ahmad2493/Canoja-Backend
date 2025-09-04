const User = require("../models/User");

async function listUsers(req, res) {
  try {
    const users = await User.find({}, "-password");
    res.json(users);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
}

module.exports = {
  listUsers,
};
