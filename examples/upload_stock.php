<?php
declare(strict_types=1);

header("Content-Type: application/json");

if ($_SERVER["REQUEST_METHOD"] !== "POST") {
    http_response_code(405);
    echo json_encode(["ok" => false, "error" => "Use POST."]);
    exit;
}

$uploadField = "file";
if (!isset($_FILES[$uploadField])) {
    http_response_code(400);
    echo json_encode(["ok" => false, "error" => "Missing uploaded file field: file"]);
    exit;
}

$uploadDir = __DIR__ . DIRECTORY_SEPARATOR . "uploads";
if (!is_dir($uploadDir) && !mkdir($uploadDir, 0775, true) && !is_dir($uploadDir)) {
    http_response_code(500);
    echo json_encode(["ok" => false, "error" => "Cannot create uploads directory."]);
    exit;
}

$timestamp = date("Ymd_His");
$targetFile = $uploadDir . DIRECTORY_SEPARATOR . "stock_export_" . $timestamp . ".csv";
$tempFile = $_FILES[$uploadField]["tmp_name"];

if (!is_uploaded_file($tempFile)) {
    http_response_code(400);
    echo json_encode(["ok" => false, "error" => "Invalid uploaded file."]);
    exit;
}

if (!move_uploaded_file($tempFile, $targetFile)) {
    http_response_code(500);
    echo json_encode(["ok" => false, "error" => "Failed to store uploaded CSV."]);
    exit;
}

// Place your ERP import call here.
// Example: call an internal script or queue a background import worker.

echo json_encode([
    "ok" => true,
    "stored_file" => basename($targetFile),
    "row_count" => $_POST["row_count"] ?? null,
    "generated_at" => $_POST["generated_at"] ?? null,
]);
