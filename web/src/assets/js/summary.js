document.addEventListener('DOMContentLoaded', function () {
    // Support multiple toggles (future-proof) and avoid duplicate-id pitfalls
    const toggles = document.querySelectorAll('input#summaryToggle');
    if (!toggles || toggles.length === 0) return;

    // New unified AI summary class system
    const aiShowWhenSummary = document.querySelectorAll('.ai-show-when-summary');
    const aiHideWhenSummary = document.querySelectorAll('.ai-hide-when-summary');

    // Apply state to the page
    function applySummaryState(showSummary) {
        // 1) Unified class-based toggling
        aiShowWhenSummary.forEach(function (el) {
            el.style.display = showSummary ? '' : 'none';
        });
        aiHideWhenSummary.forEach(function (el) {
            el.style.display = showSummary ? 'none' : '';
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
