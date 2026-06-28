# OTA Flasher — 100-Device Scale Build Plan

## What we are building and why

The current app works great for ~5 devices. At 100 devices:
- The card grid becomes a wall of scrolling cards — impossible to find a device
- There is no search bar
- You can only deploy to one device at a time
- There is no way to select multiple devices and push firmware to all of them at once

We are adding 4 features in 7 steps, one at a time, verified at each step.

---

## Feature overview

| Feature | What it does |
|---------|-------------|
| Search bar | Type a device name or ID to instantly filter the list |
| View toggle | Switch between the existing card grid and a new compact list |
| Device tags | Label devices (e.g. "RS485", "LED", "Floor 1") for context |
| Bulk deploy | Check any mix of devices → compile + OTA each one, see live progress |

---

## Files we will touch

| File | What changes |
|------|-------------|
| `src/types.rs` | Add `tags` to Device, add new types for bulk deploy |
| `src/app.rs` | Main UI and state — most changes here |
| `src/worker.rs` | No changes needed |
| `src/compile.rs` | No changes needed |
| `src/mqtt.rs` | No changes needed |

---

## Step 1 — Search bar + View toggle

**Goal:** Filter devices as you type. Toggle between card grid and compact list.

### What we add to `src/types.rs`
```rust
#[derive(Default, PartialEq, Clone, Copy)]
pub enum ViewMode { #[default] Cards, List }
```

### What we add to `src/app.rs` (App struct)
```rust
search_query: String,
view_mode: ViewMode,
```

### UI changes
- Above the device area: a search text field (left) + toggle button Cards/List (right)
- Before rendering, filter `self.fleet.devices` by `search_query` matching name or id
- If `view_mode == Cards` → existing card grid (unchanged)
- If `view_mode == List` → new list (Step 2)

### Verify
- `cargo build` — zero errors
- Launch app → search "led" → only LED devices show
- Toggle button switches view (list view shows nothing useful yet, that's Step 2)

---

## Step 2 — Compact list view

**Goal:** A dense table — one row per device, all 100 visible without endless scrolling.

### New function `render_devices_list()` in `src/app.rs`
Each row contains:
```
[ name ]  [ id ]  [ company ]  [ status dot ]  [ deployed ver ]  [ running ver ]  [ Deploy ]
```

- Rows are 28px tall (vs ~180px cards)
- 100 devices fits in ~2800px — about 3-4 screens of scrolling at most
- Status dot: green = ONLINE, grey = offline
- Deploy button on each row opens the existing single-device deploy window (unchanged)

### Verify
- `cargo build` — zero errors
- Launch app → toggle to List → see all devices in compact rows
- Deploy button on a row opens the normal deploy window

---

## Step 3 — Device tags

**Goal:** Label each device so you can see at a glance what it does.

### What we add to `src/types.rs` (Device struct)
```rust
#[serde(default)]
pub tags: Vec<String>,
```
`#[serde(default)]` means existing devices (with no tags field in JSON) load fine — they just get an empty tags list.

### UI changes
- **Add Device form**: new "Tags" text field (comma-separated, e.g. "RS485, Floor 1")
  - On save: `tags = input.split(',').map(|s| s.trim().to_string()).collect()`
- **List view row**: show tags as small coloured pill chips after the device name
- **Card view**: show tags as small pills below the device name (optional, small change)

### Verify
- `cargo build` — zero errors
- Add a new device with tags "RS485, Floor 1"
- Verify it saves and shows pills in both views
- Verify existing devices (no tags) still load without errors

---

## Step 4 — Checkboxes + multi-select

**Goal:** Select any mix of devices in list view. Show "Deploy X devices" when something is selected.

### What we add to `src/app.rs` (App struct)
```rust
selected: HashSet<String>,   // stores device IDs that are checked
```

### UI changes
- **List view rows**: add a checkbox column on the far left
  - Checking adds `device.id` to `self.selected`
  - Unchecking removes it
- **List view header row**: "Select all" checkbox — checks/unchecks all currently visible (filtered) devices
- **Top bar**: when `self.selected` is non-empty, show a highlighted button:
  `"Deploy 7 devices"` (number updates as you check/uncheck)
- Search/filter still works — selecting all only selects visible/filtered devices

### Verify
- `cargo build` — zero errors
- Launch app → list view → check 3 devices → "Deploy 3 devices" appears in top bar
- Uncheck all → button disappears
- Search filters then "select all" → only filtered devices get selected

---

## Step 5 — Bulk deploy types and events

**Goal:** Add all the data structures for bulk deploy before writing the logic. Compiles clean with no behaviour change yet.

### New types in `src/types.rs`

```rust
// State of compiling one unique sketch (multiple devices may share the same sketch)
pub enum SketchBuildState {
    Pending,
    Compiling,
    Done(PathBuf),       // .bin file ready
    Uploading,
    Uploaded(String),    // R2 URL ready
    Failed(String),
}

// Per-device progress inside a bulk deploy
pub struct DeviceBulkDeploy {
    pub device_id: String,
    pub device_name: String,
    pub sketch_dir: String,
    pub phase: DeployPhase,   // reuses existing enum
    pub log: Vec<(String, LogLevel)>,
}

// The whole bulk deploy session
pub struct BulkDeployState {
    pub deployer_name: String,
    pub new_version: String,
    pub devices: Vec<DeviceBulkDeploy>,
    // sketch_dir → build state (shared across devices with same sketch)
    pub sketch_builds: HashMap<String, SketchBuildState>,
}
```

### New AppEvent variants in `src/types.rs`
```rust
BulkCompileDone { sketch_dir: String, success: bool, bin_path: Option<PathBuf> },
BulkUploadDone  { sketch_dir: String, url: String },
BulkOtaPublished { device_id: String },
```

### New App field in `src/app.rs`
```rust
bulk_deploy: Option<BulkDeployState>,
```

### Verify
- `cargo build` — zero errors, zero warnings
- No behaviour change yet — this is just adding types

---

## Step 6 — Bulk deploy orchestration logic

**Goal:** Wire up the "Deploy X devices" button to actually compile, upload, and OTA all selected devices.

### How it works (smart deduplication)

```
Selected devices: [vit008, vit009, vit010, led3, rs485_generic]
                       ↓ group by sketch_dir
Sketch A (vit008, vit009, vit010): ~/Arduino/ESP32_VIT_MQTT_TLS/
Sketch B (led3):                   ~/Arduino/LED_Blink/
Sketch C (rs485_generic):          ~/Arduino/ESP32_RS485_MODBUS/

Step 1: Compile Sketch A, B, C in parallel (3 threads)
Step 2: Upload each binary to R2 as soon as its compile finishes
Step 3: When a binary is uploaded → OTA trigger sent to ALL devices that use that sketch
Step 4: Wait for MQTT status confirmation per device
```

Devices sharing the same sketch compile ONCE and upload ONCE — much faster.

### New function `start_bulk_deploy(deployer, version)` in `src/app.rs`
- Takes device IDs from `self.selected`
- Groups by `sketch_dir`
- Creates `BulkDeployState`
- Spawns one compile thread per unique sketch (reuses `compile::compile_sketch`)
- On `BulkCompileDone` → spawns upload (reuses `worker::upload_firmware`)
- On `BulkUploadDone` → calls `mqtt_publish` to OTA-trigger all devices for that sketch
- On MQTT `status` match → marks device Done in `bulk_deploy`

### `handle_events()` additions
```rust
AppEvent::BulkCompileDone { sketch_dir, success, bin_path } => {
    // update sketch_builds[sketch_dir] → Done(bin_path) or Failed
    // if Done: spawn upload thread
}
AppEvent::BulkUploadDone { sketch_dir, url } => {
    // update sketch_builds[sketch_dir] → Uploaded(url)
    // send OTA mqtt_publish to all devices with this sketch_dir
    // update each device phase → Waiting
}
// MqttStatus already handles per-device confirmation (reuse existing logic)
```

### Verify
- `cargo build` — zero errors
- Click "Deploy 2 devices" → verify compile threads start (check logs)
- Even if OTA doesn't reach real hardware, verify the state machine progresses

---

## Step 7 — Bulk deploy progress modal

**Goal:** A window showing live progress for every device in the bulk deploy.

### New function `render_bulk_deploy_window()` in `src/app.rs`

```
┌─────────────────────────────────────────────────────┐
│  Bulk Deploy — 5 devices                          X  │
│                                                       │
│  Done: 2  |  In progress: 2  |  Failed: 0  |  Waiting: 1  │
│                                                       │
│  ─────────────────────────────────────────────────   │
│  vit008    [████████████████████] Done v1.0.2  ✓    │
│  vit009    [████████████░░░░░░░░] Waiting...         │
│  vit010    [████░░░░░░░░░░░░░░░░] Uploading...       │
│  led3      [██████████████████░░] OTA sent...        │
│  rs485     [░░░░░░░░░░░░░░░░░░░░] Compiling...       │
│                                                       │
│  [Close — available when all done or failed]          │
└─────────────────────────────────────────────────────┘
```

- Summary line at top: Done / In progress / Failed / Waiting counts
- Per-device row: name | progress bar | phase label
- Expandable log per device (click row to expand compile/upload logs)
- Close button only enables when all devices are Done or Failed
- Cannot open another deploy while bulk is active

### Verify
- `cargo build` — zero errors
- Launch app → select 3 devices → Deploy → modal opens → watch progress update
- Confirm close button is greyed out until all finish

---

## Build sequence summary

| Step | What | Files | Risk |
|------|------|-------|------|
| 1 | Search + view toggle | app.rs, types.rs | Low |
| 2 | Compact list view | app.rs | Low |
| 3 | Device tags | types.rs, app.rs | Low |
| 4 | Checkboxes + selection | app.rs | Low |
| 5 | Bulk deploy types | types.rs, app.rs | Low (compile only) |
| 6 | Bulk deploy logic | app.rs | Medium |
| 7 | Bulk deploy modal | app.rs | Medium |

We build and verify each step before starting the next.
After every step: `cargo build` must pass with zero errors.
After steps 2, 4, 7: launch the app and visually confirm.

---

## How to follow along

After each step I will:
1. Make the code changes
2. Run `cargo build` and show you the output
3. Launch the app and take a screenshot
4. Tell you what to look at in the screenshot
5. Ask if you're happy before moving to the next step
