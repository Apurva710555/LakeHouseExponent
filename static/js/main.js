// main.js
// ================= Query Viewer Helpers =================

// Store full query text keyed by query_id
const queryTextMap = {};

// Escape HTML to avoid UI break / XSS
function escapeHtml(text) {
  if (!text) return "";
  return text.replace(
    /[&<>"']/g,
    (c) =>
      ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;",
      })[c],
  );
}

// ================= Databricks Global Helper for job monitoring =================
window.dbApi = async function (path, method = "GET", body = null) {
  const res = await fetch(path, {
    method,
    headers: {
      "Content-Type": "application/json",
    },
    body: body ? JSON.stringify(body) : null,
  });

  if (!res.ok) {
    throw new Error(`API failed: ${res.status}`);
  }

  return res.json();
};

// ------------------
// Role lists (global)
const baseRoles = [
  { value: "data_engineers", text: "Data Engineers" },
  { value: "data_scientists", text: "Data Scientists" },
  { value: "data_readers", text: "Data Readers" },
  { value: "data_stewards", text: "Data Stewards" },
];

const extraRoles = [
  { value: "administrators", text: "Administrators" },
  { value: "contributors", text: "Contributors" },
  { value: "readers", text: "Readers" },
];

const adminRoles = ["administrators", "contributors", "readers"];

// Globals to be assigned after DOM is ready
let roleDropdown = null;
let actionDropdown = null;
let appNameWrapper = null;
let rolesAreExtended = false;

// ------------------
// Utility: toggle manual vs file UI for a form section
function toggleManual(sectionPrefix) {
  const fileInput = document.getElementById(sectionPrefix + "-file");
  const manual = document.getElementById(sectionPrefix + "-manual");
  const actionRow = document.getElementById(sectionPrefix + "-action-row");
  const actionSelect = actionRow
    ? actionRow.querySelector('select[name="action"]')
    : null;
  const hasFile = !!(fileInput && fileInput.files && fileInput.files.length);

  const adminRow = document.getElementById(sectionPrefix + "-admin-row");
  if (adminRow) {
    adminRow.style.display = hasFile ? "none" : "";
    const adminInput = adminRow.querySelector('input[name="admin"]');
    if (adminInput) {
      adminInput.disabled = hasFile;
      adminInput.required = !hasFile;
    }
  }

  if (manual) {
    manual.style.display = hasFile ? "none" : "block";
    manual.querySelectorAll("input, select, textarea").forEach(function (el) {
      if (el.id === "otherType") return;
      el.disabled = hasFile;
    });
  }
  if (actionRow) {
    actionRow.style.display = hasFile ? "none" : "block";
  }
  if (actionSelect) {
    actionSelect.required = !hasFile;
    actionSelect.disabled = hasFile;
  }
}

function updateGroupFieldRequirements() {
  // preserved for compatibility - no-op as before
  return;
}

// ------------------
// Form resets
function resetUserForm() {
  const userForm = document.getElementById("userForm");
  if (userForm) userForm.reset();

  const fileInput = document.getElementById("user-file");
  if (fileInput) fileInput.value = "";

  toggleManual("user");

  const firstNameGroup = document.getElementById("user-firstname-group");
  const lastNameGroup = document.getElementById("user-lastname-group");
  const firstNameInput = document.querySelector('input[name="first_name"]');
  const lastNameInput = document.querySelector('input[name="last_name"]');
  if (firstNameGroup) firstNameGroup.style.display = "";
  if (lastNameGroup) lastNameGroup.style.display = "";
  if (firstNameInput) firstNameInput.required = true;
  if (lastNameInput) lastNameInput.required = true;
}

function resetGroupForm() {
  const groupForm = document.getElementById("groupForm");
  if (groupForm) groupForm.reset();

  const fileInput = document.getElementById("group-file");
  if (fileInput) fileInput.value = "";

  toggleManual("group");

  const adminRow = document.getElementById("group-admin-row");
  if (adminRow) {
    adminRow.style.display = "";
    const adminInput = adminRow.querySelector('input[name="admin"]');
    if (adminInput) {
      adminInput.disabled = false;
      adminInput.required = true;
    }
  }

  const otherTypeContainer = document.getElementById("otherTypeContainer");
  const otherTypeInput = document.getElementById("otherType");
  if (otherTypeContainer) otherTypeContainer.style.display = "none";
  if (otherTypeInput) {
    otherTypeInput.value = "";
    otherTypeInput.required = false;
  }

  // Reset role dropdown to base roles & pick default empty "Select"
  if (roleDropdown) {
    resetToBaseRoles();
    // ensure the empty "Select" option is selected
    roleDropdown.value = "";
    roleDropdown.selectedIndex = 0;
    rolesAreExtended = false;
    checkRoleForAppName();
  }
}

// ------------------
// Role management functions
function resetToBaseRoles() {
  if (!roleDropdown) return;

  roleDropdown.innerHTML = "";

  // Add empty Select option at top
  const emptyOpt = document.createElement("option");
  emptyOpt.value = "";
  emptyOpt.textContent = "Select";
  roleDropdown.appendChild(emptyOpt);

  baseRoles.forEach((role) => {
    const opt = document.createElement("option");
    opt.value = role.value;
    opt.textContent = role.text;
    roleDropdown.appendChild(opt);
  });

  // Make sure empty option is selected
  roleDropdown.value = "";
  roleDropdown.selectedIndex = 0;
}

function loadAddRoles() {
  if (!roleDropdown) return;
  const prior = roleDropdown.value;

  roleDropdown.innerHTML = "";

  // Add empty Select option at top
  const emptyOpt = document.createElement("option");
  emptyOpt.value = "";
  emptyOpt.textContent = "Select";
  roleDropdown.appendChild(emptyOpt);

  [...baseRoles, ...extraRoles].forEach((role) => {
    const opt = document.createElement("option");
    opt.value = role.value;
    opt.textContent = role.text;
    roleDropdown.appendChild(opt);
  });

  // restore prior selection if available and not empty, otherwise keep Select
  if (prior) {
    const opt = roleDropdown.querySelector(`option[value="${prior}"]`);
    if (opt) {
      roleDropdown.value = prior;
    } else {
      roleDropdown.value = "";
      roleDropdown.selectedIndex = 0;
    }
  } else {
    roleDropdown.value = "";
    roleDropdown.selectedIndex = 0;
  }
}

function checkRoleForAppName() {
  if (!roleDropdown || !appNameWrapper) return;
  const selectedRole = roleDropdown.value;
  appNameWrapper.style.display = adminRoles.includes(selectedRole)
    ? "block"
    : "none";
}

// ------------------
// Dashboard functions
async function loadDashboards() {
  const fetchButton = document.getElementById("fetchDashboardsBtn");
  const domainDropdown = document.getElementById("domainDropdown");
  const dashboardDropdown = document.getElementById("dashboardDropdown");
  if (!domainDropdown || !dashboardDropdown) return;

  dashboardDropdown.innerHTML = "";
  dashboardDropdown.disabled = true;
  if (fetchButton) fetchButton.disabled = true;

  try {
    const domain = domainDropdown.value;
    const res = await fetch(
      `/api/list_dashboards?domain=${encodeURIComponent(domain)}`,
    );
    if (!res.ok) throw new Error("Network response not ok");
    const data = await res.json();
    const dashboards = data.dashboard_list || [];

    dashboards.forEach((d) => {
      const opt = document.createElement("option");
      opt.value = d.id;
      // opt.text = (d.dashboard_list && d.dashboard_list.name) ? d.dashboard_list.name : (d.name || d.id);
      // Extract dashboard name
      let fullName =
        d.dashboard_list && d.dashboard_list.name
          ? d.dashboard_list.name
          : d.name || d.id;

      // Get the token that contains underscore (e.g., fleetedge_usage)
      let parts = fullName.split(/\s+/);
      let cleanName = parts.find((p) => p.includes("_")) || fullName;

      opt.text = cleanName;

      if (d.dashboard_url) opt.dataset.url = d.dashboard_url;
      dashboardDropdown.appendChild(opt);
    });

    dashboardDropdown.disabled = dashboards.length === 0;
    if (dashboardDropdown.options.length > 0)
      dashboardDropdown.selectedIndex = 0;
  } catch (error) {
    console.error("Failed to load dashboards", error);
  } finally {
    if (fetchButton) fetchButton.disabled = false;
  }

  // updateDashboard();
}

function updateDashboard() {
  const dropdown = document.getElementById("dashboardDropdown");
  const frame = document.getElementById("dashboardFrame");
  if (!dropdown || !frame) return;

  const selectedOption = dropdown.options[dropdown.selectedIndex];
  const url = selectedOption ? selectedOption.dataset.url : "";
  if (url) {
    frame.src = url;
  } else {
    frame.removeAttribute("src");
  }
}

// ------------------
// Autopopulate admin
function setAdminField(selector) {
  const input = document.querySelector(selector);
  if (!input) return;
  fetch("/me", { headers: { Accept: "application/json" } })
    .then((r) => (r.ok ? r.json() : Promise.reject()))
    .then((data) => {
      if (data && data.email) {
        input.value = data.email;
        input.readOnly = true;
        input.classList.add("bg-light");
        input.setAttribute("title", "Auto-populated from Databricks");
      }
    })
    .catch(() => {
      console.warn("Unable to auto-populate admin");
    });
}
// ================= Databricks Global Helper for job monitoring =================
// window.dbApi = async function (path, method = "GET", body = null) {
//   const res = await fetch(path, {
//     method,
//     headers: {
//       "Content-Type": "application/json"
//     },
//     body: body ? JSON.stringify(body) : null
//   });

//   if (!res.ok) {
//     throw new Error(`API failed: ${res.status}`);
//   }

//   return res.json();
// };

//------Query Time format --------

function formatDuration(seconds) {
  if (seconds == null || isNaN(seconds)) return "-";

  const hrs = Math.floor(seconds / 3600);
  const mins = Math.floor((seconds % 3600) / 60);
  const secs = Math.floor(seconds % 60);

  if (hrs > 0) {
    return `${hrs}h ${mins}m ${secs}s`;
  }
  if (mins > 0) {
    return `${mins}m ${secs}s`;
  }
  return `${secs}s`;
}

// ------------------
// Single DOMContentLoaded handler
document.addEventListener("DOMContentLoaded", function () {
  // Assign DOM nodes used globally
  roleDropdown = document.getElementById("roleDropdown");
  actionDropdown = document.getElementById("group-action");
  appNameWrapper = document.getElementById("appNameWrapper");
  loadWarehouses();

  // Initialize role dropdown and listeners if present
  if (roleDropdown) {
    resetToBaseRoles();
    roleDropdown.addEventListener("change", function () {
      checkRoleForAppName();
    });
  }

  // actionDropdown listener: matches exact "ADD_TO_GROUP"
  if (actionDropdown) {
    actionDropdown.addEventListener("change", function () {
      const action = this.value || "";
      if (action === "ADD_TO_GROUP") {
        if (!rolesAreExtended) {
          loadAddRoles();
          rolesAreExtended = true;
        }
      } else {
        if (rolesAreExtended) {
          resetToBaseRoles();
          rolesAreExtended = false;
        }
      }
      checkRoleForAppName();
    });
  }

  // Ensure appName hidden initially
  if (appNameWrapper) appNameWrapper.style.display = "none";

  // other initialization pieces (kept from original)
  const actSel = document.getElementById("group-action");
  if (actSel) {
    actSel.addEventListener("change", updateGroupFieldRequirements);
    updateGroupFieldRequirements();
  }

  toggleManual("user");
  toggleManual("group");

  setAdminField('#user-manual input[name="admin"]');
  setAdminField('#group-admin-row input[name="admin"]');

  const userActionSelect = document.querySelector(
    '#user-action-row select[name="action"]',
  );
  const firstNameGroup = document.getElementById("user-firstname-group");
  const lastNameGroup = document.getElementById("user-lastname-group");
  const firstNameInput = document.querySelector('input[name="first_name"]');
  const lastNameInput = document.querySelector('input[name="last_name"]');

  function toggleNameForDelete() {
    const actionVal = userActionSelect ? userActionSelect.value : "";
    const hideNames = actionVal === "DELETE_USER";
    if (firstNameGroup) firstNameGroup.style.display = hideNames ? "none" : "";
    if (lastNameGroup) lastNameGroup.style.display = hideNames ? "none" : "";
    if (firstNameInput) firstNameInput.required = !hideNames;
    if (lastNameInput) lastNameInput.required = !hideNames;
  }

  if (userActionSelect) {
    userActionSelect.addEventListener("change", toggleNameForDelete);
    toggleNameForDelete();
  }

  const buType = document.getElementById("business-unit");
  const otherTypeContainer = document.getElementById("otherTypeContainer");
  if (buType && otherTypeContainer) {
    const updateOtherTypeVisibility = () => {
      const otherTypeInput = document.getElementById("otherType");
      if (buType.value === "Others") {
        otherTypeContainer.style.display = "block";
        if (otherTypeInput) otherTypeInput.required = true;
      } else {
        otherTypeContainer.style.display = "none";
        if (otherTypeInput) {
          otherTypeInput.value = "";
          otherTypeInput.required = false;
        }
      }
    };
    buType.addEventListener("change", updateOtherTypeVisibility);
    updateOtherTypeVisibility();
  }

  // Dashboard wiring
  const fetchButton = document.getElementById("fetchDashboardsBtn");
  if (fetchButton) fetchButton.addEventListener("click", loadDashboards);
  // const dashboardDropdown = document.getElementById("dashboardDropdown");
  // if (dashboardDropdown) dashboardDropdown.addEventListener("change", updateDashboard);
  const loadButton = document.getElementById("loadDashboardBtn");
  if (loadButton) loadButton.addEventListener("click", updateDashboard);

  // settings modal loader
  const settingsModal = document.getElementById("settingsModal");
  if (settingsModal) {
    settingsModal.addEventListener("shown.bs.modal", async () => {
      const container = document.getElementById("settingsContent");
      if (!container) return;

      container.innerHTML = "Loading settings…";

      try {
        const res = await fetch("/settings");
        const mdText = await res.text();

        // ✅ FORCE markdown parsing
        container.innerHTML = marked.parse(mdText, {
          breaks: true,
          gfm: true,
        });
      } catch (e) {
        container.innerHTML =
          "<span class='text-danger'>Failed to load settings</span>";
      }
    });
  }

  // Help modal loader
  const helpModal = document.getElementById("helpModal");
  if (helpModal) {
    helpModal.addEventListener("shown.bs.modal", async () => {
      const container = document.getElementById("helpContent");
      if (!container) return;

      container.innerHTML = "Loading help…";

      try {
        const res = await fetch("/help");
        const mdText = await res.text();
        container.innerHTML = marked.parse(mdText);
      } catch (e) {
        container.innerHTML =
          "<span class='text-danger'>Failed to load help</span>";
      }
    });
  }

  // ------------------
  // Query Data (SQL Query Status)
  // Kill SQL Query
  window.killQuery = async function killQuery(queryId, user, originalQuery) {
    if (!queryId) {
      alert("Invalid query id");
      return;
    }

    const confirmed = confirm(
      `Are you sure you want to kill query:\n${queryId}?`,
    );

    if (!confirmed) return;

    try {
      alert(queryId);
      alert(user);
      const res = await fetch("/api/query/kill", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          query_id: queryId,
          user: user,
          query_text: originalQuery,
        }),
      });

      if (!res.ok) {
        const errText = await res.text();
        throw new Error(errText || "Failed to kill query");
      }

      alert("Query kill request submitted successfully");

      // 🔄 Refresh query list
      // document.getElementById("fetchQueryStatusBtn")?.click();
    } catch (err) {
      console.error("Kill query failed:", err);
      alert("Failed to kill query. Check logs or permissions.");
    }
  };

  const fetchQueryBtn = document.getElementById("fetchQueryStatusBtn");

  if (fetchQueryBtn) {
    fetchQueryBtn.addEventListener("click", async () => {
      const table = document.getElementById("queryStatusTable");
      const warehouseId = document.getElementById("warehouseId")?.value?.trim();
      const status = document.getElementById("queryStatus")?.value?.trim();
      const user = document.getElementById("userinfoID")?.value;
      const hours = Number(document.getElementById("timeHours").value || 0);
      const minutes = Number(document.getElementById("timeMinutes").value || 0);
      const seconds = Number(
        document.getElementById("timeSeconds")?.value || 0,
      );

      console.log("user " + user);
      console.log("status " + status);

      table.innerHTML =
        "<tr><td colspan='5' class='text-center'>Loading...</td></tr>";

      try {
        const params = new URLSearchParams();

        // ✅ ONLY send if value exists
        if (warehouseId) params.append("warehouse_id", warehouseId);
        if (status) params.append("status", status);
        if (user) params.append("user", user);
        if (hours > 0) params.append("hours", hours);
        if (minutes > 0) params.append("minutes", minutes);
        if (seconds > 0) params.append("seconds", seconds);

        const res = await fetch(`/api/query-status?${params.toString()}`);
        const data = await res.json();

        const allQueries = data;
        let currentPage = 1;
        const pageSize = 10;
        const totalRecords = data.length;
        const totalPages = Math.ceil(totalRecords / pageSize);

        const paginationInfo = document.getElementById("paginationInfo");
        // console.log("paginationInfo: " + paginationInfo);
        const paginationControls =
          document.getElementById("paginationControls");

        if (!Array.isArray(data) || data.length === 0) {
          table.innerHTML =
            "<tr><td colspan='5' class='text-muted text-center'>No queries found</td></tr>";
          return;
        }

        function renderPagination() {
          paginationControls.innerHTML = "";

          paginationControls.appendChild(
            createPageItem("«", currentPage === 1, () =>
              goToPage(currentPage - 1),
            ),
          );

          for (let i = 1; i <= totalPages; i++) {
            paginationControls.appendChild(
              createPageItem(i, false, () => goToPage(i), i === currentPage),
            );
          }

          paginationControls.appendChild(
            createPageItem("»", currentPage === totalPages, () =>
              goToPage(currentPage + 1),
            ),
          );

          updatePaginationInfo();
        }

        function createPageItem(label, disabled, onClick, active = false) {
          const li = document.createElement("li");
          li.className = `page-item ${disabled ? "disabled" : ""} ${active ? "active" : ""}`;

          const a = document.createElement("a");
          a.className = "page-link";
          a.href = "#";
          a.textContent = label;

          a.addEventListener("click", (e) => {
            e.preventDefault();
            if (!disabled) onClick();
          });

          li.appendChild(a);
          return li;
        }

        function goToPage(page) {
          if (page < 1 || page > totalPages) return;
          currentPage = page;
          renderPagination();
          renderTablePage();
        }

        function updatePaginationInfo() {
          const start = (currentPage - 1) * pageSize + 1;
          const end = Math.min(currentPage * pageSize, totalRecords);
          paginationInfo.textContent = `Showing ${start}–${end} of ${totalRecords}`;
        }

        function wireRowButtons() {
          // ================= View Query Modal Logic =================
          document.querySelectorAll(".view-query-btn").forEach((btn) => {
            btn.addEventListener("click", () => {
              const queryId = btn.dataset.queryId;
              const queryText = queryTextMap[queryId] || "Query not found";
              btn.blur();
              document.getElementById("queryViewerContent").innerText =
                queryText;

              const modal = new bootstrap.Modal(
                document.getElementById("queryViewerModal"),
              );
              modal.show();
            });
          });

          // Copy query to clipboard
          document
            .getElementById("copyQueryBtn")
            .addEventListener("click", async () => {
              const text =
                document.getElementById("queryViewerContent").innerText;
              try {
                await navigator.clipboard.writeText(text);
                document.getElementById("copyQueryBtn").innerText = "Copied";
                setTimeout(() => {
                  document.getElementById("copyQueryBtn").innerText = "Copy";
                }, 1200);
              } catch (err) {
                alert("Failed to copy query");
              }
            });

          table.querySelectorAll(".btn-terminate").forEach((btn) => {
            btn.addEventListener("click", () => {
              const queryId = btn.dataset.queryId;
              const user = btn.dataset.user;
              const originalQuery = queryTextMap[queryId]; // ✅ pulled from map

              killQuery(queryId, user, originalQuery);
            });
          });
        }

        //Function to change 'start_time' from milisecond to date
        function formatDateTime(ms) {
          if (!ms || isNaN(ms)) return "-";

          const d = new Date(Number(ms));

          return d.toLocaleString("en-IN", {
            year: "numeric",
            month: "short",
            day: "2-digit",
            hour: "2-digit",
            minute: "2-digit",
            second: "2-digit",
          });
        }

        function renderTablePage() {
          table.innerHTML = "";

          if (totalRecords === 0) {
            table.innerHTML =
              "<tr><td colspan='6' class='text-muted text-center'>No queries found</td></tr>";
            return;
          }

          const start = (currentPage - 1) * pageSize;
          const end = start + pageSize;
          const pageData = allQueries.slice(start, end);

          pageData.forEach((q) => {
            const canKill = q.status === "FINISHED";

            //logic to change the 'started at' from milisecond to date
            const milisecToDate = q.start_time;
            queryTextMap[q.query_id] =
              q.query_text || "Query text not available";

            table.innerHTML += `
      <tr>
        <td class="text-center">${escapeHtml(q.user || "-")}</td>
        <td>
          <button
            class="btn btn-sm btn-outline-secondary view-query-btn"
            data-query-id="${q.query_id}"
          >👁</button>
        </td>
        <td>${escapeHtml(q.status || "-")}</td>
        <td>${formatDateTime(q.start_time) || "-"}</td>
        <td><strong>${formatDuration(q.duration)}</strong></td>
        <td>
          ${
            canKill
              ? `<button
                  class="btn btn-sm btn-outline-danger btn-terminate"
                  data-query-id="${q.query_id}"
                  data-user="${q.user}"
                >Terminate</button>`
              : "-"
          }
        </td>
      </tr>
    `;
          });

          wireRowButtons(); // important
        }

        renderPagination();
        renderTablePage();

        // table.innerHTML = "";
        // data.forEach((q) => {
        //   const canKill = q.status === "RUNNING";
        //   // Store full SQL safely
        //   queryTextMap[q.query_id] = q.query_text || "Query text not available";
        //   table.innerHTML += `
        //     <tr>
        //       <td>${escapeHtml(q.user || "-")}</td>
        //       <td>
        //         <button
        //           class="btn btn-sm btn-outline-secondary view-query-btn"
        //           data-query-id="${q.query_id}"
        //           title="View SQL Query"
        //         >
        //           👁
        //         </button>
        //       </td>
        //       <td>${escapeHtml(q.status || "-")}</td>
        //       <td>${q.start_time || "-"}</td>
        //       <td><strong>${formatDuration(q.duration)}</strong></td>
        //       <td>
        //         ${
        //           canKill
        //             ? `<button
        //                 class="btn btn-sm btn-outline-danger btn-terminate"
        //                 data-query-id="${q.query_id}"
        //                 data-user="${q.user}"
        //               >
        //                 Terminate
        //               </button>`
        //             : "-"
        //         }
        //       </td>
        //   </tr>
        //   `;
        // });
      } catch (e) {
        // console.error(e);
        table.innerHTML =
          "<tr><td colspan='5' class='text-danger text-center'>Failed to load data</td></tr>";
      }
    });
  }

  // ---------- Dashboard toggle helpers ----------
  (function () {
    // Dummy placeholder iframe src — replace with your actual dashboard url later
    const DASHBOARD_PLACEHOLDER = "https://picsum.photos/1200/600";

    const nextBtn = document.getElementById("nextToDashboardBtn");
    const backBtn = document.getElementById("backToQueryBtn");
    const queryView = document.getElementById("queryDataView");
    const dashboardView = document.getElementById("dashboardView");
    const dashboardFrame = document.getElementById("dashboardFrame");

    // Safety checks
    if (
      !nextBtn ||
      !backBtn ||
      !queryView ||
      !dashboardView ||
      !dashboardFrame
    ) {
      // If any element is missing, do nothing silently (avoids console spam in prod)
      return;
    }

    // Show dashboard: hide query UI, load iframe if not loaded
    nextBtn.addEventListener("click", () => {
      // lazy load iframe (only set src first time)
      if (!dashboardFrame.src) {
        dashboardFrame.src = DASHBOARD_PLACEHOLDER;
      }

      queryView.classList.add("d-none");
      dashboardView.classList.remove("d-none");

      // optional: scroll to top of the card so user sees dashboard immediately
      dashboardView.scrollIntoView({ behavior: "smooth", block: "start" });
    });

    // Go back to query view
    backBtn.addEventListener("click", () => {
      dashboardView.classList.add("d-none");
      queryView.classList.remove("d-none");
      queryView.scrollIntoView({ behavior: "smooth", block: "start" });
    });
  })();

  const popoverTriggerList = [].slice.call(
    document.querySelectorAll('[data-bs-toggle="popover"]'),
  );
  popoverTriggerList.map(
    (popoverTriggerEl) => new bootstrap.Popover(popoverTriggerEl),
  );
}); // end DOMContentLoaded

 

async function loadWarehouses() {
  console.log("1111");
  const dropdown = document.getElementById("warehouseId");
  if (!dropdown) return;

  dropdown.innerHTML = `<option value="">Loading...</option>`;

  try {
    console.log("2222222");
    const res = await fetch("/api/sql/warehouses");
    console.log(res);

    if (!res.ok) throw new Error("Failed to fetch warehouses");

    const warehouses = await res.json();

    dropdown.innerHTML = `<option value="">Select Warehouse</option>`;
    warehouses.forEach((w) => {
      const opt = document.createElement("option");
      opt.textContent = w.id;
      dropdown.appendChild(opt);
    });
  } catch (err) {
    // console.error(err);
    dropdown.innerHTML = `<option value="">Failed to load</option>`;
  }
}
