const form = document.querySelector('form');

form.addEventListener('submit', (event) => {
  event.preventDefault();

  const textarea = document.getElementById('large_text_field');
  const command = textarea.value;

  fetch('/submit', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ command })
  })
  .then(response => response.json())
  .then(data => console.log(data))
  .catch(error => console.error('Error:', error));
});