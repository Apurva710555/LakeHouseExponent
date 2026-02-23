document.getElementById('notify-owner-btn').addEventListener('click', async function () {
    const btn = this;
    btn.disabled = true;
    btn.textContent = 'Notifying...';
    const resultData = JSON.parse(document.getElementById('result-data').textContent);
    const resp = await fetch('/notify-owner', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ result: resultData })
    });
    const msgDiv = document.getElementById('notify-owner-message');
    if (resp.ok) {
        const data = await resp.json();
        msgDiv.innerHTML = '<div class="alert alert-success">' + (data.message || 'Notification Sent!') + '</div>';
    } else {
        msgDiv.innerHTML = '<div class="alert alert-danger">Failed to Notify Owner.</div>';
    }
    btn.disabled = false;
    btn.textContent = 'Notify Owner';
});