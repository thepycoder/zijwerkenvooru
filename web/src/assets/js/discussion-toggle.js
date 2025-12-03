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


