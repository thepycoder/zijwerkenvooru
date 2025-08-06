document.addEventListener('DOMContentLoaded', function () {
    const toggle = document.getElementById('summaryToggle');

    const fullElements = document.querySelectorAll('.full');
    const summarizedElements = document.querySelectorAll('.summarized');
    const summaryExplanation = document.querySelector('#summaryExplanation');

    toggle.addEventListener('change', function () {
        const showSummary = this.checked;

        if (showSummary) {
            summaryExplanation.style.display = 'inline';
        } else {
            summaryExplanation.style.display = 'none';
        }

        fullElements.forEach(function (el, index) {
            const summaryEl = summarizedElements[index];

            if (showSummary && summaryEl && summaryEl.textContent.trim() !== '') {
                el.style.display = 'none';
                summaryEl.style.display = 'inline';
            } else {
                el.style.display = 'inline';
                if (summaryEl) summaryEl.style.display = 'none';
            }
        });
    });
});