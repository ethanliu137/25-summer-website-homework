document.addEventListener("DOMContentLoaded", async function () {
  // ========= 可調整：後端 API 路徑 =========
  // 若此段在 Django 模板內，可用下一行；若是外部 .js 檔，請改成字串 "/api/jobs/create/"
  const API_CREATE_JOB_URL = "{% url 'api_create_job' %}";

  // ========= 取 DOM（存在才用） =========
  const fileInput  = document.getElementById('fileInput');
  const textarea   = document.querySelector('.b_text1');
  const resultArea = document.getElementById('resultArea');
  const spinner    = document.getElementById('spinner');
  const timerEl    = document.getElementById('timer');
  const form       = document.getElementById('pmf-form');
  const ajaxBtn    = document.getElementById('ajax-btn');
  const jobSpan    = document.getElementById('jobId');
  const jobBox     = document.getElementById('jobBox');
  const extraLinks = document.getElementById('extraLinks');

  // ========= Job ID、分頁連結和結果表格管理函數 =========
  function hideJobId() {
    if (jobBox) jobBox.style.display = 'none';
    // 清除存儲的 Job ID
    localStorage.removeItem('jobShortId');
    localStorage.removeItem('jobUuid');
    sessionStorage.removeItem('currentJobShortId');
    sessionStorage.removeItem('currentJobUuid');
  }

  function hideExtraLinks() {
    if (extraLinks) extraLinks.style.display = 'none';
  }

  function hideResultArea() {
    if (resultArea) resultArea.style.display = 'none';
  }

  function showJobId(shortId) {
    if (jobSpan) jobSpan.textContent = shortId;
    if (jobBox) jobBox.style.display = 'block';
  }

  function showExtraLinks() {
    if (extraLinks) extraLinks.style.display = 'block';
  }

  function showResultArea() {
    if (resultArea) resultArea.style.display = 'block';
  }

  function hideAllResults() {
    hideJobId();
    hideExtraLinks();
    hideResultArea();
  }

  function showAllResults(shortId) {
    if (shortId) showJobId(shortId);
    showExtraLinks();
    showResultArea();
  }

  async function createNewJob() {
    try {
      console.log('Creating new job...');
      const res = await fetch("/api/jobs/create/", { 
        headers: { 'X-Requested-With': 'XMLHttpRequest' } 
      });
      if (!res.ok) {
        const txt = await res.text().catch(() => String(res.status));
        console.error('Job creation failed:', res.status, txt);
        return null;
      }
      const data = await res.json();
      console.log('New job created:', data);

      if (!data || !data.short_id || !data.job_id) {
        console.error('Invalid job data received:', data);
        return null;
      }

      // 存到 sessionStorage（關閉瀏覽器就消失）
      sessionStorage.setItem('currentJobShortId', data.short_id);
      sessionStorage.setItem('currentJobUuid', data.job_id);
      
      return data;
    } catch (e) {
      console.error('Error creating job:', e);
      return null;
    }
  }

  // ========= 初始化時隱藏所有結果相關元素 =========
  hideAllResults(); // 頁面載入時先隱藏所有結果相關元素

  // ---------- Helper ----------
  function escapeHtml(str) {
    return String(str).replace(/[&<>"'`=\/]/g, s => ({
      '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;','/':'&#x2F;','`':'&#x60;','=':'&#x3D;'
    }[s]));
  }

  function rowsToRecords(columns, rows) {
    if (!Array.isArray(columns) || !Array.isArray(rows)) return [];
    return rows.map(r => {
      const obj = {};
      columns.forEach((name, i) => { obj[name] = Array.isArray(r) ? r[i] : r?.[i]; });
      return obj;
    });
  }

  function whenDataTablesReady(fn, timeoutMs = 5000) {
    const start = Date.now();
    (function tick(){
      const ok = (window.jQuery && window.$ && $.fn && $.fn.dataTable);
      if (ok) { try { fn(); } catch(e){ console.error(e); } return; }
      if (Date.now() - start > timeoutMs) { console.warn("DataTables not ready in time"); return; }
      setTimeout(tick, 50);
    })();
  }

  // ---------- 轉圈 + 計時 ----------
  let timerId = null, elapsed = 0;
  function startSpinner() {
    if (!spinner) return;
    elapsed = 0;
    if (timerEl) timerEl.textContent = `已經過 ${elapsed} 秒`;
    spinner.style.display = 'block';
    if (timerId) clearInterval(timerId);
    timerId = setInterval(() => {
      elapsed += 1;
      if (timerEl) timerEl.textContent = `已經過 ${elapsed} 秒`;
    }, 1000);
  }
  function stopSpinner() {
    if (!spinner) return;
    spinner.style.display = 'none';
    if (timerId) { clearInterval(timerId); timerId = null; }
  }

  // ---------- 若有 resultArea，預先放容器（但保持隱藏）----------
  if (resultArea) {
    resultArea.innerHTML = '<table id="resultsTable" class="display" style="width:100%"></table>';
    hideResultArea(); // 確保一開始是隱藏的
  }

  // ---------- 若有 lastResult，等 DataTables 就渲染（但先隱藏）----------
  const last = localStorage.getItem("lastResult");
  if (last && resultArea) {
    const cachedPayload = JSON.parse(last);
    // 先準備表格容器但保持隱藏
    resultArea.innerHTML = '<table id="resultsTable" class="display" style="width:100%"></table>';
    whenDataTablesReady(() => {
      renderTable(cachedPayload);
      // 如果有快取結果，顯示所有元素（但不顯示 Job ID，因為是舊的）
      showResultArea();
      showExtraLinks();
    });
  }

  // ---------- 檔案選擇（存在才綁） ----------
  if (fileInput && textarea) {
    fileInput.addEventListener('change', (e) => {
      const files = Array.from(e.target.files || []);
      const fastaFiles = files.filter(f => /\.fa(sta)?$/i.test(f.name));
      if (fastaFiles.length !== files.length) alert('請只上傳 FASTA 檔案（.fasta 或 .fa）');
      if (fastaFiles.length > 0) {
        const reader = new FileReader();
        reader.onload = (ev) => { textarea.value = ev.target.result; };
        reader.readAsText(fastaFiles[0]);
      }
    });
  }

  // ---------- AJAX 提交（存在才綁）---------- 
  if (ajaxBtn && form && resultArea) {
    ajaxBtn.addEventListener('click', async () => {
      // 1. 清除之前的結果和 Job ID
      resultArea.innerHTML = '<table id="resultsTable" class="display" style="width:100%"></table>';
      hideJobId();
      
      // 2. 開始處理（不顯示 Job ID）
      startSpinner();
      ajaxBtn.disabled = true;

      // 3. 創建新的 Job ID（但先不顯示）
      const jobData = await createNewJob();

      const formData = new FormData(form);
      try {
        const res = await fetch(form.action, {
          method: 'POST',
          headers: { 'X-Requested-With': 'XMLHttpRequest' },
          body: formData
        });
        if (!res.ok) throw new Error(await res.text());
        const payload = await res.json();
        localStorage.setItem("lastResult", JSON.stringify(payload));
        
        // 4. 先渲染表格，完成後再顯示 Job ID
        whenDataTablesReady(() => {
          renderTable(payload);
          // 表格渲染完成後才顯示 Job ID
          if (jobData) {
            showJobId(jobData.short_id);
          }
        });
        
      } catch (err) {
        console.error(err);
        resultArea.innerHTML = `<p style="color:red">發生錯誤：${escapeHtml(err.message || String(err))}</p>`;
        // 錯誤時不顯示 Job ID
        hideJobId();
      } finally {
        stopSpinner();
        ajaxBtn.disabled = false;
      }
    });
  }

  // ---------- 渲染 DataTable ----------
  function renderTable(payload) {
    if (!resultArea) return;

    let records = payload?.records ?? payload?.data ?? [];
    const rows  = payload?.rows || [];
    let columns = payload?.columns || [];

    if (!columns.length && records && records.length) columns = Object.keys(records[0]);
    if ((!records || !records.length) && Array.isArray(rows) && rows.length && columns.length) {
      records = rowsToRecords(columns, rows);
    }
    if (!columns.length) {
      resultArea.innerHTML = `<p style="opacity:.8">沒有資料可顯示</p>`;
      return;
    }

    // 清理舊表
    if ($.fn.dataTable.isDataTable('#resultsTable')) {
      $('#resultsTable').DataTable().clear().destroy();
    }
    $('#resultsTable').remove();
    resultArea.innerHTML = '<table id="resultsTable" class="display" style="width:100%"></table>';

    const dtColumns = columns.map(name => ({ title: name, data: name }));

    $('#resultsTable').DataTable({
      stateSave: true,
      stateDuration: 0,
      data: records,
      columns: dtColumns,
      pageLength: 10,
      lengthMenu: [10, 25, 50, 100],
      searching: true,
      ordering: true,
      responsive: true,
      deferRender: true,
      scrollX: true,
      scrollCollapse: true,
      autoWidth: false,
      dom: 'Bfrtip',
      buttons: ['csv'],
      language: {
        lengthMenu: "Show _MENU_ entries per page",
        search: "Search:",
        info: "Showing _START_ to _END_ of _TOTAL_ entries",
        infoEmpty: "No records available",
        paginate: { first: "First", last: "Last", next: "Next", previous: "Previous" }
      }
    });
  }
});