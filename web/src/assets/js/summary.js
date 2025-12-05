document.addEventListener('DOMContentLoaded', function () {
    // Support multiple toggles (future-proof) and avoid duplicate-id pitfalls
    const toggles = document.querySelectorAll('input#summaryToggle');
    if (!toggles || toggles.length === 0) return;

    // Apply state to the page (CSS class based)
    function applySummaryState(showSummary) {
        if (showSummary) {
            document.documentElement.classList.add('ai-summary-enabled');
        } else {
            document.documentElement.classList.remove('ai-summary-enabled');
        }
    }

    // Initialize from persisted value
    // Note: Visual state is handled by inline scripts in head/navbar to prevent flicker.
    // We just ensure JS state matches here in case inline script failed or for robustness.
    const STORAGE_KEY = 'summaryToggleChecked.v2';
    const saved = localStorage.getItem(STORAGE_KEY);
    const initialChecked = saved === 'true';

    // Sync toggle elements
    toggles.forEach(t => { t.checked = initialChecked; });
    // Ensure class is correct (redundant if inline script worked, but good for consistency)
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

// Discussion Toggle Logic
document.addEventListener("DOMContentLoaded", () => {
    const buttons = document.querySelectorAll(".toggle-discussion");
    if (!buttons || buttons.length === 0) return;

    buttons.forEach((button) => {
        button.addEventListener("click", () => {
            // Try to find the card that contains the button
            const card = button.closest(".card");

            let discussion = null;
            // Preferred: the .discussion immediately following the toggle card
            if (card && card.nextElementSibling && card.nextElementSibling.classList.contains("discussion")) {
                discussion = card.nextElementSibling;
            } else {
                // Fallback: search within the nearest question container
                const container = button.closest(".question-container") || button.closest(".question-card-wrapper") || document;
                discussion = container.querySelector(".discussion");
            }

            if (!discussion) return;

            const isOpen = discussion.classList.toggle("open");
            button.classList.toggle("rotated", isOpen);
        });
    });
});
