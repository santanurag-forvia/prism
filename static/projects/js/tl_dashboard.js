document.addEventListener('DOMContentLoaded', function() {
  // Chart refs
  let teamCapacityChart, reporteeUtilChart, deviationLineChart, fteProjectChart, selfAllocChart;

  // Populate filter dropdowns
  function populateFilters(years, months, programs) {
    const ySel = document.getElementById('filterYear');
    const mSel = document.getElementById('filterMonth');
    const pSel = document.getElementById('filterProgram');
    ySel.innerHTML = years.map(y => `<option value="${y}">${y}</option>`).join('');
    mSel.innerHTML = months.map(m => `<option value="${m.value}">${m.label}</option>`).join('');
    pSel.innerHTML = `<option value="">All Programs</option>` + programs.map(p => `<option value="${p}">${p}</option>`).join('');
    // Set defaults
    ySel.value = years[years.length-1];
    mSel.value = (new Date().getMonth()+1).toString();
  }

  // Fetch and render all charts
  async function loadDashboard() {
    const year = document.getElementById('filterYear').value;
    const month = document.getElementById('filterMonth').value;
    const program = document.getElementById('filterProgram').value;
    const params = new URLSearchParams({year, month, program});

    const res = await fetch(`/dashboard/api/tl_dashboard_data/?${params}`);
    if (!res.ok) return;
    const data = await res.json();

    // Team Capacity vs Actual Load
    if (teamCapacityChart) teamCapacityChart.destroy();
    teamCapacityChart = new Chart(document.getElementById('teamCapacityChart'), {
      type: 'bar',
      data: {
        labels: ['Team'],
        datasets: [
          { label: 'Capacity (FTE)', data: [data.team_capacity], backgroundColor: '#60a5fa' },
          { label: 'Actual Load (FTE)', data: [data.team_actual], backgroundColor: '#0ea5e9' }
        ]
      },
      options: { responsive: true, plugins: { legend: { position: 'top' } }, indexAxis: 'y' }
    });

    // Reportee vs Utilization
    if (reporteeUtilChart) reporteeUtilChart.destroy();
    reporteeUtilChart = new Chart(document.getElementById('reporteeUtilChart'), {
      type: 'bar',
      data: {
        labels: data.reportees.map(r => r.name),
        datasets: [
          { label: 'Planned FTE', data: data.reportees.map(r => r.planned_fte), backgroundColor: '#a3e635' },
          { label: 'Actual FTE', data: data.reportees.map(r => r.actual_fte), backgroundColor: '#fbbf24' }
        ]
      },
      options: { responsive: true, plugins: { legend: { position: 'top' } }, }
    });

    // Deviation Line Chart
    if (deviationLineChart) deviationLineChart.destroy();
    deviationLineChart = new Chart(document.getElementById('deviationLineChart'), {
      type: 'line',
      data: {
        labels: data.deviation.labels,
        datasets: [
          { label: 'Planned Hours', data: data.deviation.planned, borderColor: '#6366f1', fill: false },
          { label: 'Consumed Hours', data: data.deviation.consumed, borderColor: '#f43f5e', fill: false }
        ]
      },
      options: { responsive: true, plugins: { legend: { position: 'top' } } }
    });

    // FTE Utilization by Project/Subproject
    if (fteProjectChart) fteProjectChart.destroy();
    fteProjectChart = new Chart(document.getElementById('fteProjectChart'), {
      type: 'bar',
      data: {
        labels: data.fte_projects.labels,
        datasets: [
          { label: 'FTE', data: data.fte_projects.fte, backgroundColor: '#38bdf8' }
        ]
      },
      options: { responsive: true, plugins: { legend: { display: false } }, }
    });

    // Self Allocation
    if (selfAllocChart) selfAllocChart.destroy();
    selfAllocChart = new Chart(document.getElementById('selfAllocChart'), {
      type: 'line',
      data: {
        labels: data.self_alloc.labels,
        datasets: [
          { label: 'FTE Consumed', data: data.self_alloc.fte, borderColor: '#10b981', fill: true, backgroundColor: 'rgba(16,185,129,0.12)' }
        ]
      },
      options: { responsive: true, plugins: { legend: { position: 'top' } } }
    });
  }

  // Initial load: fetch filter options and data
  fetch('/dashboard/api/tl_dashboard_filters/')
    .then(r => r.json())
    .then(({years, months, programs}) => {
      populateFilters(years, months, programs);
      loadDashboard();
      // Bind filter change
      document.getElementById('filterYear').onchange = loadDashboard;
      document.getElementById('filterMonth').onchange = loadDashboard;
      document.getElementById('filterProgram').onchange = loadDashboard;
    });
});