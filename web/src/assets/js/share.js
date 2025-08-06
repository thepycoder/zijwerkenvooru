document.addEventListener('DOMContentLoaded', () => {
    const shareButton = document.getElementById('share-button');
  
    if (navigator.share && shareButton) {
      shareButton.addEventListener('click', async () => {
        try {
          await navigator.share({
            title: document.title,
            text: 'Bekijk deze vraag!',
            url: window.location.href,
          });
          console.log('Pagina gedeeld!');
        } catch (err) {
          console.error('Fout bij delen:', err);
        }
      });
    } else if (shareButton) {
      shareButton.style.display = 'none';
    }
  });
  