document.addEventListener('DOMContentLoaded', function () {
    // Support multiple toggles (future-proof) and avoid duplicate-id pitfalls
    const toggles = document.querySelectorAll('input#summaryToggle');
    if (!toggles || toggles.length === 0) return;

    // Textual summaries following the .full / .summarized pairing pattern
    const fullElements = document.querySelectorAll('.full');
    // Discussion summaries (AI) shown alongside the full discussion
    const discussionContainers = document.querySelectorAll('.discussion');
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

        // 1) Toggle .full /.summarized text blocks (titles, topics, etc.)
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

        // 2) Toggle AI discussion summaries (keep full discussion visible)
        discussionContainers.forEach(function (container) {
            // The AI summary container lives as a message-container sibling within .discussion
            const aiSummaryContainer = container.querySelector('.ai-discussion-summary');
            if (!aiSummaryContainer) return;

            // Only show the AI block when toggled ON and when there is non-empty content
            const aiSummaryHtml = aiSummaryContainer.querySelector('.ai-summary-html');
            const hasContent = aiSummaryHtml && aiSummaryHtml.textContent.trim() !== '';

            if (showSummary && hasContent) {
                // Show AI summary block; do NOT hide the rest of the discussion
                aiSummaryContainer.style.display = '';
            } else {
                // Hide the AI summary block
                aiSummaryContainer.style.display = 'none';
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