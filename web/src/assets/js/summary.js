document.addEventListener('DOMContentLoaded', function () {
    // Support multiple toggles (future-proof) and avoid duplicate-id pitfalls
    const toggles = document.querySelectorAll('input#summaryToggle');
    if (!toggles || toggles.length === 0) return;

    const fullElements = document.querySelectorAll('.full');
    const summaryExplanation = document.querySelector('#summaryExplanation');

    function findSummaryElementFor(fullEl) {
        if (fullEl.nextElementSibling && fullEl.nextElementSibling.classList.contains('summarized')) {
            return fullEl.nextElementSibling;
        }
        if (fullEl.previousElementSibling && fullEl.previousElementSibling.classList.contains('summarized')) {
            return fullEl.previousElementSibling;
        }
        if (fullEl.parentElement) {
            // fallback: search within the same parent container
            return fullEl.parentElement.querySelector('.summarized');
        }
        return null;
    }

    // Apply state to the page
    function applySummaryState(showSummary) {
        if (summaryExplanation) {
            summaryExplanation.style.display = showSummary ? 'inline' : 'none';
        }
        fullElements.forEach(function (el) {
            const summaryEl = findSummaryElementFor(el);

            const hasSummary = summaryEl && summaryEl.textContent.trim() !== '';
            if (showSummary && hasSummary) {
                el.style.display = 'none';
                summaryEl.style.display = 'inline';
            } else {
                el.style.display = 'inline';
                if (summaryEl) summaryEl.style.display = 'none';
            }
        });
    }

    // Initialize from persisted value (bump key to default OFF for all users)
    const STORAGE_KEY = 'summaryToggleChecked.v2';
    const saved = localStorage.getItem(STORAGE_KEY);
    const initialChecked = saved === 'true';
    toggles.forEach(t => { t.checked = initialChecked; });
    applySummaryState(initialChecked);

    // Persist on change and apply
    toggles.forEach(toggle => {
        toggle.addEventListener('change', function () {
            const showSummary = this.checked;
            // Sync all toggles to the same state
            toggles.forEach(t => { if (t !== toggle) t.checked = showSummary; });
            localStorage.setItem(STORAGE_KEY, showSummary ? 'true' : 'false');
            applySummaryState(showSummary);
        });
    });
});