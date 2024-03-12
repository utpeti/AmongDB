async function sendSomething(event) {
  event.preventDefault(); // Prevent the form from submitting normally
  const text = document.getElementById('large_text_field').value;
  const res = await fetch('/api/databases', {
      method: 'POST',
      headers: {
          'Content-Type': 'application/json'
      },
      body: JSON.stringify({ "text": text })
  });
  document.getElementById('large_text_field').value = '';
  // const myjson = await res.json();
  // console.log(myjson);
}