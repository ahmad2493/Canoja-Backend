const express = require("express");
const router = express.Router();
const adminController = require("../controllers/adminController");

/**
 * @swagger
 * /api/admin/users:
 *   get:
 *     summary: Get all users
 *     description: Fetches all users from the database while excluding sensitive fields like password.
 *     tags:
 *       - Admin
 *     responses:
 *       200:
 *         description: List of users retrieved successfully
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 type: object
 *                 properties:
 *                   _id:
 *                     type: string
 *                     example: "64ff3e3b2f4c3d1b2a8c9f1a"
 *                   name:
 *                     type: string
 *                     example: "John Doe"
 *                   email:
 *                     type: string
 *                     example: "john@example.com"
 *       500:
 *         description: Internal server error
 */
router.get("/users", adminController.listUsers);

module.exports = router;
