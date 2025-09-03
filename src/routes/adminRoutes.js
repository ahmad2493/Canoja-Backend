const express = require("express");
const router = express.Router();
const adminController = require("../controllers/adminController");

router.post("/login", adminController.login);
router.get("/users", adminController.listUsers);
router.post("/users", adminController.createUser);
router.patch("/users/:userId/role", adminController.changeUserRole);

module.exports = router;
