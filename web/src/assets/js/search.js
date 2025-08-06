function labelShortcut() {
    const els = Array.from(document.body.querySelectorAll('.meta-key-symbol'));
    for (const el of els) {
        el.textContent = navigator.platform.includes('Mac')
            ? '⌘K'
            : 'Ctrl+K';
    }
}
const isMobile = /iPhone|iPad|iPod|Android/i.test(navigator.userAgent);
const isApple = /apple/i.test(navigator.vendor);
const searchModal = document.body.querySelector('#search-modal');
const searchModalTrigger = document.body.querySelector('#search-button');
const dialog = new A11yDialog(searchModal);
const searchBox = searchModal.querySelector("#search-box");
labelShortcut();
addEventsToDOMAPIDialog(searchModal);
new PagefindUI({
    element: searchBox,
    showSubResults: false,
    showImages: true,
    resetStyles: false,
    excerptLength: 20,
    translations: {
        placeholder: "Waar ben je naar op zoek?",
        zero_results: "Geen resultaten voor \"[SEARCH_TERM]\""
    }
});
const input = searchModal
    .querySelector(".pagefind-ui__search-input")
    searchModal
    .addEventListener(
        "show",
        () => { // if (window.bodyScrollLock.disableBodyScroll) { window.bodyScrollLock.disableBodyScroll(searchDialog) }
            document.documentElement.removeEventListener("keydown", listenForShowShortcut)
            if (! isMobile || ! isApple) {
                input.focus()
            }
        }
    )
    searchModal
    .addEventListener("hide", () => {
        input.value = ""
        input.dispatchEvent(new Event("input", {bubbles: true}));
        document.documentElement.addEventListener("keydown", listenForShowShortcut)
        // if (window.bodyScrollLock.enableBodyScroll) {
        //     window.bodyScrollLock.enableBodyScroll(searchDialog)
        // }
    })
function addEventsToDOMAPIDialog(el) {
    const showEvent = new CustomEvent("show");
    const hideEvent = new CustomEvent("hide");
    const observer = new MutationObserver((mutationList) => {
        mutationList.forEach(({attributeName, target}) => {
            if (attributeName !== "aria-hidden") {
                return
            }
            if (target.getAttribute("aria-hidden") === "true") {
                target.dispatchEvent(hideEvent);
                return
            }
            target.dispatchEvent(showEvent);
        });
    });
    observer.observe(el, {attributes: true});
}
function listenForShowShortcut(event) {
    const {isComposing, key} = event
    if (isComposing || event.getModifierState("Alt")) {
        return
    }
    const slash = key === "/" && ! event.getModifierState("Control") && ! event.getModifierState("Meta")
    const ctrlOrCmdK = key === "k" && (event.getModifierState("Control") || event.getModifierState("Meta"))
    if (! slash && ! ctrlOrCmdK) {
        return
    }
    event.preventDefault()
    dialog.show()
    // window?.fathom?.trackEvent(`Search Dialog|Show|${slash ? 'Slash' : 'Ctrl- or Cmd-K'}`);
}
// $(document).ready(function () {
//
//
//     searchModalTrigger.click(function (ev) {
//         ev.preventDefault();
//
//         // MicroModal.show('search-modal', {
//         //     onClose: function () {
//         //         // $('.nav-link-contact').blur();
//         //     },
//         //     disableFocus: true
//         // });
//
//         document.querySelector('.pagefind-ui__search-input').focus();
//     });
//
// }) ;